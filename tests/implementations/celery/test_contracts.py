"""Contract compliance tests for the Celery backend.

Inherits the shared contract suite and binds it to the real
``CeleryRateLimiter`` via the ``limiter`` fixture in conftest.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestCeleryContracts(RateLimiterContractTest):
    """Verify ``CeleryRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Re-expose the conftest limiter under the contract fixture name."""
        return limiter
