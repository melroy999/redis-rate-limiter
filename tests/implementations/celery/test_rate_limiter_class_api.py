"""Celery-specific class API behavior tests.

Core class-API behavior is covered by `tests/implementations/test_rate_limiter_class_api.py`.
This module keeps only tests that are specific to Celery backend context.
"""

import pytest

from celery_rate_limiter.backends.celery.limiter import CeleryRateLimiter


class TestCeleryRateLimiterClassApi:
    """Celery-only tests for class API/backend context behavior."""

    @staticmethod
    def _configure(redis_client, celery_app) -> None:
        CeleryRateLimiter._reset()
        CeleryRateLimiter.configure(redis_client, celery_app=celery_app)

    def test_configure_sets_class_state(self, redis_client, celery_app):
        """Verify configure stores Redis and Celery app class context."""
        # Act
        self._configure(redis_client, celery_app)

        # Assert
        assert CeleryRateLimiter._redis_client is redis_client, (
            "configure should store the provided Redis client"
        )
        assert CeleryRateLimiter._celery_app is celery_app, (
            "configure should store the provided Celery app"
        )

    def test_configure_without_celery_app_raises_error(self, redis_client):
        """Verify configure fails when celery_app is missing."""
        # Arrange
        CeleryRateLimiter._reset()

        # Act & Assert
        with pytest.raises(RuntimeError, match="celery_app"):
            CeleryRateLimiter.configure(redis_client)

    def test_reset_clears_cached_instances_and_backend_context(
        self, redis_client, celery_app, default_limiter_id
    ):
        """Verify _reset clears local cache and backend context."""
        # Arrange
        self._configure(redis_client, celery_app)
        CeleryRateLimiter.create(
            default_limiter_id,
            limit=5,
            window=60,
            max_concurrency=2,
            override=True,
        )

        # Act
        CeleryRateLimiter._reset()

        # Assert
        assert CeleryRateLimiter._instances == {}, (
            "reset should clear local limiter cache"
        )
        assert CeleryRateLimiter._redis_client is None, (
            "reset should clear shared redis client"
        )
        assert CeleryRateLimiter._celery_app is None, (
            "reset should clear shared celery app"
        )

    def test_direct_construction_raises_runtime_error(
        self, redis_client, celery_app, default_limiter_id
    ):
        """Verify direct constructor usage is rejected."""
        # Act & Assert
        with pytest.raises(RuntimeError, match="Direct CeleryRateLimiter"):
            CeleryRateLimiter(
                redis_client=redis_client,
                celery_app=celery_app,
                limiter_id=default_limiter_id,
                limit=5,
                window=60,
                max_concurrency=2,
            )

    def test_class_api_create_succeeds(
        self, redis_client, celery_app, default_limiter_id
    ):
        """Verify class API create path remains valid."""
        # Arrange
        self._configure(redis_client, celery_app)

        # Act
        created = CeleryRateLimiter.create(
            default_limiter_id,
            limit=10,
            window=60,
            max_concurrency=5,
            override=True,
        )

        # Assert
        assert created.id == default_limiter_id, (
            "create should return the requested limiter instance"
        )
