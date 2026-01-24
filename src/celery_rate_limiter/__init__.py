from celery import Celery
from redis import Redis

from celery_rate_limiter.registry import RateLimiterRegistry
from celery_rate_limiter.limiters import CeleryRateLimiter


class CeleryRateLimiterFactory:
    """Factory for creating Celery rate limiter instances."""

    def __init__(
        self,
        redis_client: Redis,
        celery_app: Celery,
    ):
        """
        Initialize the Celery rate limiter factory.
        The factory ensures that all rate limiters use the same redis and app instance.
        Additionally, the rate limit registry is factory specific.
        """
        self.redis = redis_client
        self.app = celery_app
        self.registry = RateLimiterRegistry(self.redis, self.app)

    def create_limiter(
            self, limiter_id, window: int, limit: int, max_concurrency: int, max_age: int = 3600,
            lease_duration: int = 30, override: bool = False, persist: bool = True
    ) -> CeleryRateLimiter:
        """
        Create a Celery rate limiter instance with the given parameters.
        :param limiter_id: The id of the rate limiter to create.
        :param window: The time window in seconds that the limit is applied to.
        :param limit: The maximum number of tasks per time window.
        :param max_concurrency: The maximum number of concurrent tasks.
        :param max_age: The maximum time a task may exist in the queue before it expires.
        :param lease_duration: The time in seconds after which the leash to a concurrency slot will expire.
        :param override: Whether to forcefully overwrite an existing rate limiter configuration.
        :param persist: Whether to persist the rate limiter configuration by storing it in Redis.
        :return: A rate limiter using the desired parameters.
        """
        limiter = CeleryRateLimiter(
            redis_client=self.redis,
            celery_app=self.app,
            limiter_id=limiter_id,
            window=window,
            limit=limit,
            max_concurrency=max_concurrency,
            max_age=max_age,
            lease_duration=lease_duration
        )
        self.registry.register(limiter, override, persist)
        return limiter