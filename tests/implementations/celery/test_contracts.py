"""Contract compliance tests for the Celery backend.

This module inherits the shared contract suite and binds it to the
``CeleryRateLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.

Fixture dependencies:
    - ``limiter``: from ``tests/implementations/celery/conftest.py``.
    - ``_reset_limiter_class_state``: autouse fixture from ``tests/fixtures/celery_backend.py``.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter


class TestCeleryContracts(RateLimiterContractTest):
    """Verify that ``CeleryRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Wrap the sync Celery limiter in an async adapter for the unified contracts."""
        # Pause the drain loop far into the future to prevent it from consuming
        # tasks before the contract assertions inspect the buffer.
        limiter._paused_until = 5_000_000_000.0
        return SyncToAsyncLimiterAdapter(limiter)
