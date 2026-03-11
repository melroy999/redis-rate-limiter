"""Contract compliance tests for the ProcessPool backend.

This module inherits the shared contract suite and binds it to the
``ProcessPoolRateLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.

Fixture dependencies:
    - ``redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``limiter``, ``_reset_limiter_class_state``: from
      ``tests/implementations/processpool/conftest.py``.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter


@pytest.mark.contract
class TestProcessPoolContracts(RateLimiterContractTest):
    """Verify that ``ProcessPoolRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Wrap the sync ProcessPool limiter in an async
        adapter for the unified contracts."""
        return SyncToAsyncLimiterAdapter(limiter)
