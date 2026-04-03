"""ASGI rate limiter backend.

A lightweight, request-oriented rate limiter that performs a "try acquire" check
per HTTP request using the sliding window counter algorithm. One limiter instance
handles all identities via dynamic keys (e.g., ``{limiter_id}:{client_ip}``).
"""

from __future__ import annotations

import logging
import time
from typing import Any, ClassVar

import redis.asyncio
import redis.exceptions

from redis_rate_limiter.backends.asgi.types import AcquireResult
from redis_rate_limiter.core.base import AbstractAsyncRateLimiter
from redis_rate_limiter.core.managed import AsyncManagedRateLimiter

logger = logging.getLogger(__name__)


class ASGIRateLimiter(AsyncManagedRateLimiter, AbstractAsyncRateLimiter):
    """Rate limiter for ASGI middleware, backed by the sliding window counter algorithm.

    Unlike the distributed task limiters, the ASGI limiter does not use buffer,
    concurrency, DLQ, or task lifecycle machinery. It provides a single
    ``acquire(key)`` method that atomically checks and increments the rate limit
    for the given identity.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.
    """

    _refresh_interval: ClassVar[float] = 5.0

    # ---------------------------------------------------------------------------
    # Managed backend hooks
    # ---------------------------------------------------------------------------

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Accept optional backend-specific context.

        The ASGI limiter does not require an external executor or app; this
        method is effectively a no-op but satisfies the abstract interface.
        """
        pass

    @classmethod
    def _has_backend_context(cls) -> bool:
        return True

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def _reset_backend_context(cls) -> None:
        pass

    @classmethod
    def _configure_hint(cls) -> str:
        return "ASGIRateLimiter.configure(redis_client)"

    # ---------------------------------------------------------------------------
    # Instance construction
    # ---------------------------------------------------------------------------

    def __init__(
        self,
        redis_client: redis.asyncio.Redis,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct an ASGI rate limiter instance through the managed class API.

        Args:
            redis_client: The async Redis client used for state management.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractAsyncRateLimiter``
        and ``AbstractRateLimiter`` (i.e., ``limiter_id``, ``limit``, ``window``).
        """
        super().__init__(redis_client, _sentinel=_sentinel, **kwargs)
        self._last_refresh: float = 0.0

    async def start(self) -> None:
        """Register the acquire Lua script with the Redis server.

        Called by ``AsyncManagedRateLimiter.create()`` and ``.get()`` after
        construction.
        """
        await super().start()

        # Eagerly preload the Lua script so the first acquire() avoids a lazy registration round-trip.
        await self._register_script("acquire.lua")
        logger.info(
            "[ASGIRateLimiter] Initialized: id=%s, limit=%d, window_s=%g.",
            self.id,
            self.limit,
            self.window,
        )

    # ---------------------------------------------------------------------------
    # Core API
    # ---------------------------------------------------------------------------

    async def acquire(self, key: str) -> AcquireResult:
        """Attempt to acquire a rate limit token for the given identity.

        Constructs a dynamic Redis key ``{limiter_id}:{key}`` and executes the
        ``acquire.lua`` sliding window counter check. Periodically refreshes
        configuration from Redis (at most once every ``_refresh_interval`` seconds).

        Args:
            key: The identity string (e.g., client IP, API key).

        Returns:
            An ``AcquireResult`` indicating whether the request is allowed and
            the remaining token count.
        """
        # Throttled config refresh.
        now = time.monotonic()
        if now - self._last_refresh >= self._refresh_interval:
            self._last_refresh = now
            await self.refresh_config()

        full_key = f"{self.id}:{key}"

        try:
            result = await self._eval_script(
                "acquire.lua", 1, full_key, self.window, self.limit
            )

            return AcquireResult(
                allowed=int(result[0]) == 1,
                remaining=int(result[1]),
                reset_in_ms=int(result[2]),
                val_previous=int(result[3]),
                val_current=int(result[4]),
            )
        except Exception:
            logger.exception(
                "[ASGIRateLimiter] Acquire failed: limiter=%s, key=%s.",
                self.id,
                key,
            )
            raise
