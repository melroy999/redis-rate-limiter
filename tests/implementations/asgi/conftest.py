"""Fixtures for ASGI rate limiter and middleware tests."""

import pytest

from celery_rate_limiter.backends.asgi import ASGIRateLimiter


@pytest.fixture(autouse=True)
async def _reset_asgi_limiter_class_state(async_redis_client):
    """Ensure that the ASGI limiter class state is clean before and after each test."""
    ASGIRateLimiter._reset()
    ASGIRateLimiter.configure(async_redis_client)
    yield
    ASGIRateLimiter._reset()


@pytest.fixture
async def limiter(async_redis_client, limiter_id):
    """Create an ASGIRateLimiter instance for testing."""
    _limiter = await ASGIRateLimiter.create(
        limiter_id=f"{limiter_id}_asgi",
        limit=10,
        window=60,
        override=True,
    )
    yield _limiter
