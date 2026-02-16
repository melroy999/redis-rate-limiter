"""Contract compliance tests for the Celery backend.

This module inherits the shared contract suite and binds it to the
``CeleryRateLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestCeleryContracts(RateLimiterContractTest):
    """Verify that ``CeleryRateLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    def limiter(self, limiter):
        """Re-expose the conftest-provided limiter under the contract fixture name."""
        return limiter
