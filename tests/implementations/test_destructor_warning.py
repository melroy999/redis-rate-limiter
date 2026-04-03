"""Tests for ``__del__`` ResourceWarning emission on sync and async rate limiters.

Tests are written once in async form via the mixin pattern; the sync
implementation participates directly (``__del__`` is synchronous on both
classes, so no ``SyncToAsyncLimiterAdapter`` is needed).

Fixture dependencies:
    - ``redis_client``, ``async_redis_client``,
      ``limiter_id``: from ``tests/conftest.py``.
    - ``StubRateLimiter``, ``AsyncStubRateLimiter``:
      from ``tests/implementations/conftest.py``.
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
        """Verify that ``__del__`` emits a ResourceWarning
        when shutdown was not called.
        """
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
    async def test_del_handles_missing_drain_loop_attribute(limiter_drain_disabled):
        """Verify that ``__del__`` does not raise when ``_drain_loop``
        has not been set (e.g., partial initialization failure).

        Mutation target: ``getattr(self, "_drain_loop", None)`` default
        argument in ``__del__``.
        """
        # Arrange
        limiter = limiter_drain_disabled
        if hasattr(limiter, "_drain_loop"):
            delattr(limiter, "_drain_loop")

        # Act & Assert
        # Should not raise AttributeError.
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            limiter.__del__()

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


@pytest.mark.behavior
class TestSyncDestructorWarning(DestructorWarningTests):
    """Sync rate limiter ``__del__`` ResourceWarning tests."""

    @pytest.fixture
    def limiter_with_drain(self, redis_client, limiter_id):
        """Create a sync limiter with drain enabled that has not been shut down."""
        from tests.implementations.conftest import StubRateLimiter

        limiter = StubRateLimiter(
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
        from tests.implementations.conftest import StubRateLimiter

        return StubRateLimiter(
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
        from tests.implementations.conftest import StubRateLimiter

        limiter = StubRateLimiter(
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


@pytest.mark.behavior
class TestAsyncDestructorWarning(DestructorWarningTests):
    """Async rate limiter ``__del__`` ResourceWarning tests."""

    @pytest.fixture
    async def limiter_with_drain(self, async_redis_client, limiter_id):
        """Create an async limiter with drain enabled that has not been shut down."""
        from tests.implementations.conftest import AsyncStubRateLimiter

        limiter = AsyncStubRateLimiter(
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
        from tests.implementations.conftest import AsyncStubRateLimiter

        return AsyncStubRateLimiter(
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
        from tests.implementations.conftest import AsyncStubRateLimiter

        limiter = AsyncStubRateLimiter(
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


# ---------------------------------------------------------------------------
# Unified boundary tests
# ---------------------------------------------------------------------------


class DestructorWarningBoundaryTests:
    """Boundary condition tests for ``__del__`` warning stacklevel.

    Subclasses must provide the following fixtures:
        - ``limiter_with_drain``: a limiter with ``drain_enabled=True`` that has
          NOT been shut down.
        - ``source_filename``: the basename of the source file that defines
          ``__del__`` (e.g., ``"limiters.py"``).
    """

    @staticmethod
    async def test_del_warning_stacklevel_points_to_source(
        limiter_with_drain, source_filename
    ):
        """Verify that the warning's filename points to the source
        module where ``__del__`` is defined, not the caller."""
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
        assert resource_warnings[0].filename.endswith(source_filename), (
            f"warning filename should point to {source_filename}, "
            f"got {resource_warnings[0].filename}"
        )


# ---------------------------------------------------------------------------
# Concrete boundary test cases
# ---------------------------------------------------------------------------


@pytest.mark.behavior
class TestSyncDestructorWarningBoundary(DestructorWarningBoundaryTests):
    """Sync rate limiter ``__del__`` stacklevel boundary tests."""

    @pytest.fixture
    def limiter_with_drain(self, redis_client, limiter_id):
        """Create a sync limiter with drain enabled that has not been shut down."""
        from tests.implementations.conftest import StubRateLimiter

        limiter = StubRateLimiter(
            redis_client=redis_client,
            limiter_id=f"{limiter_id}_del_boundary_sync",
            limit=5,
            window=60,
            max_concurrency=2,
        )
        yield limiter
        limiter.shutdown()

    @pytest.fixture
    def source_filename(self):
        """Return the expected source filename for the sync limiter."""
        return "limiters.py"


@pytest.mark.behavior
class TestAsyncDestructorWarningBoundary(DestructorWarningBoundaryTests):
    """Async rate limiter ``__del__`` stacklevel boundary tests."""

    @pytest.fixture
    async def limiter_with_drain(self, async_redis_client, limiter_id):
        """Create an async limiter with drain enabled that has not been shut down."""
        from tests.implementations.conftest import AsyncStubRateLimiter

        limiter = AsyncStubRateLimiter(
            redis_client=async_redis_client,
            limiter_id=f"{limiter_id}_del_boundary_async",
            limit=5,
            window=60,
            max_concurrency=2,
        )
        await limiter.start()
        yield limiter
        await limiter.shutdown()

    @pytest.fixture
    def source_filename(self):
        """Return the expected source filename for the async limiter."""
        return "async_limiters.py"
