"""Contract compliance tests for the ThreadPool backend.

Inherits the shared contract suite and binds it to the real
``ThreadPoolRateLimiter`` via the ``limiter`` fixture in conftest.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestThreadPoolContracts(RateLimiterContractTest):
    """Verify ``ThreadPoolRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Re-expose the conftest limiter under the contract fixture name."""
        return limiter
