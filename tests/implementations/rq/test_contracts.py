"""Contract compliance tests for the RQ backend.

This module inherits the shared contract suite and binds it to the
``RQRateLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.

Fixture dependencies:
    - ``limiter``: from ``tests/implementations/rq/conftest.py``.
    - ``_reset_limiter_class_state``: autouse fixture
      from ``tests/fixtures/rq_backend.py``.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter


@pytest.mark.contract
class TestRQContracts(RateLimiterContractTest):
    """Verify that ``RQRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Wrap the sync RQ limiter in an async adapter
        for the unified contracts."""
        return SyncToAsyncLimiterAdapter(limiter)
