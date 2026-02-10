"""Shared fixtures for property-based tests."""

import pytest

from celery_rate_limiter.limiters import CeleryRateLimiter


@pytest.fixture(scope="module")
def property_redis_client(_redis_connection):
    """Module-scoped Redis client for property-based tests."""
    yield _redis_connection
    _redis_connection.flushdb()


@pytest.fixture(scope="module")
def property_celery_app(celery_config):
    """Module-scoped Celery app for property-based tests."""
    from celery import Celery

    app = Celery("property_test_app")
    app.config_from_object(celery_config)
    return app


@pytest.fixture(scope="module")
def property_limiter(
    property_redis_client,
    property_celery_app,
    default_module_limiter_id,
):
    """Default module-scoped limiter for property-based tests."""
    return CeleryRateLimiter(
        redis_client=property_redis_client,
        celery_app=property_celery_app,
        limiter_id=f"{default_module_limiter_id}_property_default",
        limit=100,
        window=60,
        max_concurrency=50,
        max_age=3600,
        lease_duration=30,
    )
