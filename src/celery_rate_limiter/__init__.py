import logging

from celery import Celery
from redis import Redis

from celery_rate_limiter.limiters import CeleryRateLimiter
from celery_rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)


class CeleryRateLimiterFactory:
    """Factory for creating Celery rate limiter instances."""

    def __init__(
        self,
        redis_client: Redis,
        celery_app: Celery,
    ):
        """Initialize the Celery rate limiter factory.

        The factory ensures that all rate limiters use the same redis and app instance.
        Additionally, the rate limit registry is factory specific.

        Args:
            redis_client: The Redis client to use for all limiters.
            celery_app: The Celery app to use for all limiters.
        """
        self.redis = redis_client
        self.app = celery_app
        self.registry = RateLimiterRegistry(self.redis, self.app)
        logger.info("CeleryRateLimiterFactory initialized.")

    def create_limiter(
        self,
        limiter_id: str,
        window: int,
        limit: int,
        max_concurrency: int,
        max_age: int = 3600,
        lease_duration: int = 30,
        override: bool = False,
        persist: bool = True,
    ) -> CeleryRateLimiter:
        """Create a Celery rate limiter instance with the given parameters.

        Args:
            limiter_id: The id of the rate limiter to create.
            window: The time window in seconds that the limit is applied to.
            limit: The maximum number of tasks per time window.
            max_concurrency: The maximum number of concurrent tasks.
            max_age: The maximum time a task may exist in the queue before it expires.
            lease_duration: The time in seconds after which the lease to a concurrency slot will expire.
            override: Whether to forcefully overwrite an existing rate limiter configuration.
            persist: Whether to persist the rate limiter configuration by storing it in Redis.

        Returns:
            A rate limiter using the desired parameters.
        """
        limiter = CeleryRateLimiter(
            redis_client=self.redis,
            celery_app=self.app,
            limiter_id=limiter_id,
            window=window,
            limit=limit,
            max_concurrency=max_concurrency,
            max_age=max_age,
            lease_duration=lease_duration,
        )
        self.registry.register(limiter, override, persist)
        logger.info(
            "Limiter created via factory: limiter_id=%s, window_s=%d, limit=%d, max_concurrency=%d, max_age_s=%d, lease_duration_s=%d, override=%s, persist=%s.",
            limiter_id,
            window,
            limit,
            max_concurrency,
            max_age,
            lease_duration,
            override,
            persist,
        )
        return limiter
