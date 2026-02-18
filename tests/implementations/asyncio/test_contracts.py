"""Contract compliance tests for the AsyncIO backend.

This module inherits the shared contract suite and binds it to the
``AsyncIOTaskLimiter`` implementation via the ``asyncio_limiter`` fixture
defined in the accompanying conftest module.
"""

import pytest

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestAsyncIOContracts(RateLimiterContractTest):
    """Verify that ``AsyncIOTaskLimiter`` satisfies all rate limiter contracts."""

    @pytest.fixture
    async def limiter(self, asyncio_limiter):
        """Re-expose the conftest-provided limiter under the contract fixture name."""
        return asyncio_limiter
