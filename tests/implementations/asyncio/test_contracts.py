"""Contract compliance tests for the AsyncIO backend.

This module inherits the shared contract suite and binds it to the
``AsyncIOTaskLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.
"""

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestAsyncIOContracts(RateLimiterContractTest):
    """Verify that ``AsyncIOTaskLimiter`` satisfies all rate limiter contracts."""

    pass
