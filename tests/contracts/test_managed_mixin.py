"""Contract tests for the ``ManagedRateLimiterMixin`` interface.

These tests verify two contracts:

1. **Abstract hook enforcement**: the base mixin must raise ``NotImplementedError``
   for all five abstract backend hooks when a subclass does not override them.

2. **Configure hint compliance**: every concrete backend must return a
   ``_configure_hint()`` string that starts with its own class name, so that
   runtime error messages correctly identify the backend to the user.

Fixture dependencies:
    None. All tests are self-contained.
"""

import pytest

from redis_rate_limiter.backends.asgi.limiter import ASGIRateLimiter
from redis_rate_limiter.backends.asyncio.limiter import AsyncIOTaskLimiter
from redis_rate_limiter.backends.celery.limiter import CeleryRateLimiter
from redis_rate_limiter.backends.processpool.limiter import ProcessPoolRateLimiter
from redis_rate_limiter.backends.rq.limiter import RQRateLimiter
from redis_rate_limiter.backends.threading.limiter import ThreadPoolRateLimiter
from redis_rate_limiter.core.managed import ManagedRateLimiterMixin


class _BareMixin(ManagedRateLimiterMixin):
    """Subclass that implements none of the abstract backend hooks.

    Used by ``TestAbstractBackendHooks`` to verify that the base mixin
    properly enforces the interface contract via ``NotImplementedError``.
    """


class TestAbstractBackendHooks:
    """Contract: unoverridden abstract hooks must raise ``NotImplementedError``."""

    @staticmethod
    def test_configure_backend_raises_not_implemented():
        """Contract: ``_configure_backend`` must raise ``NotImplementedError`` when not overridden."""
        # Act & Assert
        with pytest.raises(NotImplementedError, match="^Subclasses"):
            _BareMixin._configure_backend()

    @staticmethod
    def test_has_backend_context_raises_not_implemented():
        """Contract: ``_has_backend_context`` must raise ``NotImplementedError`` when not overridden."""
        # Act & Assert
        with pytest.raises(NotImplementedError, match="^Subclasses"):
            _BareMixin._has_backend_context()

    @staticmethod
    def test_get_instance_context_raises_not_implemented():
        """Contract: ``_get_instance_context`` must raise ``NotImplementedError`` when not overridden."""
        # Act & Assert
        with pytest.raises(NotImplementedError, match="^Subclasses"):
            _BareMixin._get_instance_context()

    @staticmethod
    def test_reset_backend_context_raises_not_implemented():
        """Contract: ``_reset_backend_context`` must raise ``NotImplementedError`` when not overridden."""
        # Act & Assert
        with pytest.raises(NotImplementedError, match="^Subclasses"):
            _BareMixin._reset_backend_context()

    @staticmethod
    def test_configure_hint_raises_not_implemented():
        """Contract: ``_configure_hint`` must raise ``NotImplementedError`` when not overridden."""
        # Act & Assert
        with pytest.raises(NotImplementedError, match="^Subclasses"):
            _BareMixin._configure_hint()


class TestConfigureHintCompliance:
    """Contract: ``_configure_hint()`` must start with the backend class name."""

    @staticmethod
    @pytest.mark.parametrize(
        "backend_cls",
        [
            ASGIRateLimiter,
            AsyncIOTaskLimiter,
            CeleryRateLimiter,
            ProcessPoolRateLimiter,
            RQRateLimiter,
            ThreadPoolRateLimiter,
        ],
        ids=lambda cls: cls.__name__,
    )
    def test_configure_hint_starts_with_class_name(backend_cls):
        """Contract: ``_configure_hint()`` must return a hint starting with the class name."""
        # Act
        hint = backend_cls._configure_hint()

        # Assert
        assert hint.startswith(backend_cls.__name__), (
            f"configure hint must start with the class name {backend_cls.__name__}"
        )
