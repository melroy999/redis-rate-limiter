"""Contract compliance tests for the AsyncIO backend.

This module inherits the shared contract suite and binds it to the
``AsyncIOTaskLimiter`` implementation via the ``limiter`` fixture
defined in the accompanying conftest module.

Fixture dependencies:
    - ``async_redis_client``, ``limiter_id``: from ``tests/conftest.py``.
    - ``limiter``, ``_reset_asyncio_limiter_class_state``: from ``tests/implementations/asyncio/conftest.py``.
"""

from tests.contracts.test_rate_limiter import RateLimiterContractTest


class TestAsyncIOContracts(RateLimiterContractTest):
    """Verify that ``AsyncIOTaskLimiter`` satisfies all rate limiter contracts."""

    pass
