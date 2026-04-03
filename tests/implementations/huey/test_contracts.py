"""Contract compliance tests for the Huey backend.

This module inherits the shared contract suite and binds it to the
``HueyRateLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.

Fixture dependencies:
    - ``limiter``: from ``tests/implementations/huey/conftest.py``.
    - ``_reset_limiter_class_state``: autouse fixture
      from ``tests/fixtures/huey_backend.py``.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest
from tests.helpers.adapters import SyncToAsyncLimiterAdapter


@pytest.mark.contract
class TestHueyContracts(RateLimiterContractTest):
    """Verify that ``HueyRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Wrap the sync Huey limiter in an async adapter
        for the unified contracts."""
        return SyncToAsyncLimiterAdapter(limiter)
