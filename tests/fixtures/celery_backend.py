"""Shared fixtures for the Celery backend test suites."""

import os

import pytest

from redis_rate_limiter import CeleryRateLimiter
from tests.helpers.utils import cleanup_managed_limiter
from tests.implementations.conftest import DEFAULT_LIMITER_CONFIG

# Redis configuration is derived from environment variables.
# The default values target localhost:6379, but may be overridden for Docker Compose.
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


@pytest.fixture(scope="session")
def celery_config():
    """Provide the configuration for the celery_app fixture.

    The same Redis configuration used by the tests (derived from environment
    variables) is applied here to ensure consistency.

    Returns:
        A dictionary containing the Celery configuration for testing.
    """
    redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    return {
        "broker_url": redis_url,
        "result_backend": redis_url,
        "task_always_eager": True,
    }


@pytest.fixture(scope="session")
def celery_app(celery_config):
    """Provide a Celery application instance configured from the ``celery_config`` fixture."""
    from celery import Celery

    app = Celery("celery_backend_test_app")
    app.config_from_object(celery_config)
    return app


# noinspection PyProtectedMember
@pytest.fixture(autouse=True)
def _reset_limiter_class_state(redis_client, celery_app):
    """Reset the CeleryRateLimiter class-level state before and after each test.

    This is performed to prevent singleton cache pollution between tests and
    to ensure that configure() is invoked with the appropriate test fixtures.
    """
    CeleryRateLimiter._reset()
    CeleryRateLimiter.configure(redis_client, celery_app=celery_app)
    yield
    CeleryRateLimiter._reset()


@pytest.fixture
def limiter(redis_client, limiter_id, _reset_limiter_class_state):
    """Perform setup and teardown for a CeleryRateLimiter instance.

    Yields:
        A configured CeleryRateLimiter instance ready for testing.
    """
    # Setup
    test_limiter = CeleryRateLimiter.create(
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
        override=True,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()
    cleanup_managed_limiter(redis_client, limiter_id)
