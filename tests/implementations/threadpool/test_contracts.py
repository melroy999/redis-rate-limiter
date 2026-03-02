"""Contract compliance tests for the ThreadPool backend.

This module inherits the shared contract suite and binds it to the
``ThreadPoolRateLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``limiter``, ``_reset_limiter_class_state``: from ``tests/implementations/threadpool/conftest.py``.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter


class TestThreadPoolContracts(RateLimiterContractTest):
    """Verify that ``ThreadPoolRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Wrap the sync ThreadPool limiter in an async adapter for the unified contracts."""
        # Pause the drain loop far into the future to prevent it from consuming
        # tasks before the contract assertions inspect the buffer.
        limiter._paused_until = 5_000_000_000.0
        return SyncToAsyncLimiterAdapter(limiter)
