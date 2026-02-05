import json
from typing import Dict

from celery import Celery
from redis import Redis

from .limiters import CeleryRateLimiter


class RateLimiterRegistry:
    """A registry for rate limiter instances."""

    def __init__(
        self,
        redis_client: Redis,
        celery_app: Celery,
    ):
        self._limiters: Dict[str, CeleryRateLimiter] = {}
        self.app = celery_app
        self.redis = redis_client

    def register(
        self, limiter: CeleryRateLimiter, override: bool = False, persist: bool = True
    ) -> None:
        """Register a limiter instance by its limiter_id.

        Args:
            limiter: The limiter to register. The name of the limiter will be used as the key.
            override: Whether to overwrite existing limiters.
            persist: Whether the rate limiter configuration should be stored in the redis database.

        Raises:
            ValueError: If the limiter ID is already registered and override is False.
        """
        # Check whether we are about to make an illegal override of an existing configuration.
        # Raise an exception if the operation will overwrite an existing limiter (unless force=True).
        if not override and limiter.id in self:
            raise ValueError(
                f"Limiter ID '{limiter.id}' is already registered. "
                "If this is intentional (e.g., in tests), use override=True."
            )

        # Register the limiter locally and, if required, in redis storage as well.
        self._limiters[limiter.id] = limiter
        if persist:
            config = {
                "window": limiter.window,
                "limit": limiter.limit,
                "max_concurrency": limiter.max_concurrency,
                "max_age": limiter.max_age,
                "lease_duration": limiter.lease_duration,
            }
            # This updates the source of truth that all workers watch.
            self.redis.hset("rl:registry:configs", limiter.id, json.dumps(config))

    def get(self, limiter_id: str) -> CeleryRateLimiter:
        """Get a limiter by its ID from local storage or Redis storage.

        Args:
            limiter_id: The id of the limiter to get.

        Returns:
            The limiter associated with the id.

        Raises:
            ValueError: If the limiter ID doesn't exist.
        """
        # Return locally cached limiters as-is.
        if limiter_id in self._limiters:
            return self._limiters[limiter_id]

        # Check if the limiter is available in the redis store.
        raw_config = str(self.redis.hget("rl:registry:configs", limiter_id))
        if raw_config:
            # Avoid circular dependencies through a lazy import.
            from .limiters import CeleryRateLimiter

            config = json.loads(raw_config)

            # Reconstruct the rate limiter.
            instance = CeleryRateLimiter(self.redis, self.app, limiter_id, **config)

            # Store the rate limiter and return.
            self._limiters[limiter_id] = instance
            return instance

        raise ValueError(
            f"Limiter with ID '{limiter_id}' not found in local registry or Redis storage. "
            "Ensure it was registered at startup."
        )

    def __contains__(self, limiter_id: str) -> bool:
        """Check if a limiter exists in either local or Redis storage by its ID.

        Args:
            limiter_id: The id of the limiter to check.

        Returns:
            True if the limiter exists, False otherwise.
        """
        return limiter_id in self._limiters or bool(
            self.redis.hexists("rl:registry:configs", limiter_id)
        )

    def list_all(self) -> list[str]:
        """Returns all registered limiter IDs."""
        return list(self._limiters.keys())
        # self.redis.hkeys("rl:registry:configs")

    def clear(self) -> None:
        """Clear all registered limiters."""
        self._limiters.clear()
