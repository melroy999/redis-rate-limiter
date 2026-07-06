"""Shared fixtures for property-based tests."""

import pytest

from tests.implementations.conftest import StubRateLimiter


@pytest.fixture(scope="module")
def property_redis_client(_redis_connection):
    """Provide a module-scoped Redis client for property-based tests."""
    yield _redis_connection


@pytest.fixture(scope="module")
def make_property_limiter(property_redis_client, module_limiter_id):
    """Create a ``StubRateLimiter`` with a unique suffix and optional config overrides."""
    created = []

    def _factory(suffix: str, **overrides) -> StubRateLimiter:
        defaults: dict = dict(
            limit=10, window=1.0, max_concurrency=5, max_age=3600, lease_duration=30
        )
        defaults.update(overrides)
        limiter = StubRateLimiter(
            redis_client=property_redis_client,
            limiter_id=f"{module_limiter_id}_property_{suffix}",
            **defaults,
        )
        created.append(limiter)
        return limiter

    yield _factory

    for limiter in created:
        limiter.shutdown()
