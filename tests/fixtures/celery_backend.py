"""Shared fixtures for Celery backend test suites."""

import os

import pytest

from celery_rate_limiter.backends.celery.limiter import CeleryRateLimiter

# Redis configuration from environment variables.
# Defaults to localhost:6379, but can be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def celery_config():
    """Configure the celery_app fixture.

    Uses the same Redis configuration as tests (from environment variables).

    Returns:
        A dictionary with Celery configuration for testing.
    """
    redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    return {
        "broker_url": redis_url,
        "result_backend": redis_url,
        "task_always_eager": True,
    }


@pytest.fixture(scope="session")
def celery_app(celery_config):
    """Provide a Celery app configured from ``celery_config``."""
    from celery import Celery

    app = Celery("celery_backend_test_app")
    app.config_from_object(celery_config)
    return app


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, celery_app):
    """Reset CeleryRateLimiter class-level state before and after each test.

    This prevents singleton cache pollution between tests and ensures
    configure() is called with the test fixtures.
    """
    CeleryRateLimiter._reset()
    CeleryRateLimiter.configure(redis_client, celery_app=celery_app)
    yield
    CeleryRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, celery_app, default_limiter_id):
    """Setup and teardown for the CeleryRateLimiter.

    Yields:
        A configured CeleryRateLimiter instance for testing.
    """
    # Setup
    limiter_id = default_limiter_id
    test_limiter = CeleryRateLimiter.create(
        limiter_id=limiter_id,
        limit=5,
        window=60,
        max_concurrency=2,
        max_age=3600,
        lease_duration=30,
        override=True,
    )

    yield test_limiter

    # Teardown
    # Clear keys associated with this limiter.
    keys = redis_client.keys(f"{limiter_id}:*")
    if keys:
        redis_client.delete(*keys)
