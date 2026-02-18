"""Contract compliance tests for the AsyncIO backend.

This module inherits the shared async contract suite and binds it to the
``AsyncIOTaskLimiter`` implementation via the ``asyncio_limiter`` fixture
defined in the accompanying conftest module.
"""

import pytest

from tests.contracts.test_rate_limiter_async import AsyncRateLimiterContractTest


class TestAsyncIOContracts(AsyncRateLimiterContractTest):
    """Verify that ``AsyncIOTaskLimiter`` satisfies all async rate limiter contracts."""

    @pytest.fixture
    async def limiter(self, asyncio_limiter):
        """Re-expose the conftest-provided limiter under the contract fixture name."""
        return asyncio_limiter
