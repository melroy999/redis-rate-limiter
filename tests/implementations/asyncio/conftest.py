"""Fixtures for AsyncIO task limiter tests."""

import pytest

from celery_rate_limiter.backends.asyncio import AsyncIOTaskLimiter


@pytest.fixture(autouse=True)
async def _reset_asyncio_limiter_class_state(async_redis_client):
    """Ensure that the AsyncIO limiter class state is clean before and after each test."""
    AsyncIOTaskLimiter._reset()
    AsyncIOTaskLimiter.configure(async_redis_client, max_tasks=4)
    yield
    AsyncIOTaskLimiter._reset()


@pytest.fixture
async def asyncio_limiter(async_redis_client, default_limiter_id):
    """Create an AsyncIOTaskLimiter instance for testing."""
    limiter = await AsyncIOTaskLimiter.create(
        limiter_id=f"{default_limiter_id}_asyncio",
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
        override=True,
    )
    yield limiter
    await limiter.shutdown()
