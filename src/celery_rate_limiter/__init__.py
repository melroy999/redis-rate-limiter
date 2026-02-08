import logging
import warnings

from celery import Celery
from redis import Redis

from celery_rate_limiter.limiters import CeleryRateLimiter
from celery_rate_limiter.registry import RateLimiterRegistry

logger = logging.getLogger(__name__)

__all__ = ["CeleryRateLimiter", "CeleryRateLimiterFactory"]


class CeleryRateLimiterFactory:
    """Factory for creating Celery rate limiter instances.

    .. deprecated::
        Use ``CeleryRateLimiter.configure()`` + ``CeleryRateLimiter.create()`` instead.
    """

    def __init__(
        self,
        redis_client: Redis,
        celery_app: Celery,
    ):
        """Initialize the Celery rate limiter factory.

        .. deprecated::
            Use ``CeleryRateLimiter.configure(redis_client, celery_app)`` instead.

        Args:
            redis_client: The Redis client to use for all limiters.
            celery_app: The Celery app to use for all limiters.
        """
        warnings.warn(
            "CeleryRateLimiterFactory is deprecated. "
            "Use CeleryRateLimiter.configure(redis_client, celery_app) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.redis = redis_client
        self.app = celery_app
        self.registry = RateLimiterRegistry(self.redis, self.app)

        # Also configure the new class-level API for forward compatibility.
        CeleryRateLimiter.configure(redis_client, celery_app)
        logger.info("CeleryRateLimiterFactory initialized (deprecated).")

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

        .. deprecated::
            Use ``CeleryRateLimiter.create()`` instead.

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
        warnings.warn(
            "CeleryRateLimiterFactory.create_limiter() is deprecated. "
            "Use CeleryRateLimiter.create() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        limiter = CeleryRateLimiter.create(
            limiter_id=limiter_id,
            limit=limit,
            window=window,
            max_concurrency=max_concurrency,
            max_age=max_age,
            lease_duration=lease_duration,
            override=override,
            persist=persist,
        )
        # Also register in the legacy registry for backward compatibility.
        self.registry._limiters[limiter_id] = limiter
        logger.info(
            "Limiter created via deprecated factory: limiter_id=%s.",
            limiter_id,
        )
        return limiter
