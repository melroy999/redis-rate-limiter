"""Abstract base classes for the rate limiter hierarchy.

This module defines three layers of abstraction:

- ``AbstractRateLimiter``: configuration-only base; stores ``limiter_id``, ``limit``, and ``window``.
- ``AbstractSyncRateLimiter``: adds synchronous Redis connectivity and script management.
- ``AbstractAsyncRateLimiter``: adds asynchronous Redis connectivity and script management.

Concrete backends compose these bases with mixins (``DistributedRateLimiterMixin``,
``ManagedRateLimiterMixin``) via multiple inheritance.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Awaitable, cast

import redis
import redis.asyncio
from redis import Redis

from celery_rate_limiter.core.scripts import load_lua_script

logger = logging.getLogger(__name__)


class AbstractRateLimiter:
    """Configuration-only base for all rate limiters.

    Stores the limiter identity and the core rate limit parameters. All
    constructor parameters are keyword-only to prevent positional conflicts
    in cooperative multiple inheritance chains.
    """

    id: str
    limit: int
    window: float

    def __init__(
        self,
        *,
        limiter_id: str,
        limit: int,
        window: float,
        **kwargs: Any,
    ) -> None:
        self.id = limiter_id
        self.limit = limit
        self.window = window
        self._config_version: int = 0
        self._paused_until: float = 0.0

        # Guards against unconsumed kwargs, i.e., a TypeError will be thrown
        # by object.__init__() if kwargs is non-empty.
        super().__init__(**kwargs)

    def _build_persist_config(self) -> dict[str, Any]:
        """Return the configuration dictionary suitable for Redis persistence.

        Subclasses should call ``super()._build_persist_config()`` and extend
        the result with backend-specific fields.
        """
        return {"limit": self.limit, "window": self.window}

    def _apply_config_overrides(self, overrides: dict[str, Any]) -> None:
        """Apply configuration overrides, handling window-change pausing.

        Subclasses should call ``super()._apply_config_overrides(overrides)``
        to ensure base fields are handled before processing backend-specific ones.
        """
        new_window = overrides.get("window")
        if new_window is not None and new_window != self.window:
            pause_duration = max(self.window, new_window)
            self._paused_until = time.time() + pause_duration
            self.window = float(new_window)
            logger.info(
                "Window change detected: limiter=%s, new_window=%g, paused_for_s=%g.",
                self.id,
                new_window,
                pause_duration,
            )

        new_limit = overrides.get("limit")
        if new_limit is not None:
            self.limit = int(new_limit)


class AbstractSyncRateLimiter(AbstractRateLimiter):
    """Synchronous Redis rate limiter base.

    Provides script loading, SHA caching, and ``evalsha`` execution with
    automatic ``NOSCRIPT`` recovery. All Redis operations are synchronous.
    """

    def __init__(self, redis_client: Redis, **kwargs: Any) -> None:
        self.redis: Redis = redis_client
        self._script_shas: dict[str, str] = {}
        self._script_sources: dict[str, str] = {}
        super().__init__(**kwargs)

    def _register_script(self, script_name: str) -> str:
        """Load a Lua script from disk and register it with the Redis server.

        The script source and SHA are cached for subsequent use by
        ``_eval_script``. Calling this method multiple times for the same
        script name re-uploads the script to Redis and updates the cached SHA.

        Args:
            script_name: The filename of the Lua script (e.g., ``"consume.lua"``).

        Returns:
            The SHA1 hash of the registered script.
        """
        if script_name not in self._script_sources:
            self._script_sources[script_name] = load_lua_script(script_name)

        sha = str(self.redis.script_load(self._script_sources[script_name]))
        self._script_shas[script_name] = sha
        logger.debug(
            "Lua script registered: limiter=%s, script=%s, sha=%s.",
            self.id,
            script_name,
            sha,
        )
        return sha

    def _eval_script(self, script_name: str, num_keys: int, *args: Any) -> Any:
        """Execute a cached Lua script via ``EVALSHA`` with NOSCRIPT recovery.

        If the script SHA is not yet cached, the script is lazily registered.
        On ``NOSCRIPT`` errors (e.g., after a Redis restart), the script is
        re-registered and the call is retried once.

        Args:
            script_name: The filename of the Lua script.
            num_keys: The number of Redis keys in the argument list.
            *args: The keys and arguments to pass to the script.

        Returns:
            The result of the Lua script execution.

        Raises:
            RuntimeError: If the script cannot be registered after the retry.
        """
        if script_name not in self._script_shas:
            self._register_script(script_name)

        sha = self._script_shas[script_name]
        try:
            return self.redis.evalsha(sha, num_keys, *args)
        except redis.exceptions.NoScriptError:
            logger.warning(
                "Lua script cache miss; reloading: limiter=%s, script=%s.",
                self.id,
                script_name,
            )
            self._register_script(script_name)
            new_sha = self._script_shas[script_name]
            try:
                return self.redis.evalsha(new_sha, num_keys, *args)
            except redis.exceptions.NoScriptError:
                raise RuntimeError(
                    f"Redis failed to retain the Lua script '{script_name}' after reload."
                )


# noinspection PyUnnecessaryCast
class AbstractAsyncRateLimiter(AbstractRateLimiter):
    """Asynchronous Redis rate limiter base.

    Provides async script loading, SHA caching, and ``EVALSHA`` execution
    with automatic ``NOSCRIPT`` recovery. All Redis operations use
    ``redis.asyncio.Redis``.
    """

    def __init__(self, redis_client: redis.asyncio.Redis, **kwargs: Any) -> None:
        self.redis: redis.asyncio.Redis = redis_client
        self._script_shas: dict[str, str] = {}
        self._script_sources: dict[str, str] = {}
        super().__init__(**kwargs)

    async def _register_script(self, script_name: str) -> str:
        """Load a Lua script from disk and register it with the Redis server.

        Args:
            script_name: The filename of the Lua script (e.g., ``"acquire.lua"``).

        Returns:
            The SHA1 hash of the registered script.
        """
        if script_name not in self._script_sources:
            self._script_sources[script_name] = load_lua_script(script_name)

        sha = str(await self.redis.script_load(self._script_sources[script_name]))
        self._script_shas[script_name] = sha
        logger.debug(
            "Lua script registered (async): limiter=%s, script=%s, sha=%s.",
            self.id,
            script_name,
            sha,
        )
        return sha

    async def _eval_script(self, script_name: str, num_keys: int, *args: Any) -> Any:
        """Execute a cached Lua script via ``EVALSHA`` with NOSCRIPT recovery.

        Args:
            script_name: The filename of the Lua script.
            num_keys: The number of Redis keys in the argument list.
            *args: The keys and arguments to pass to the script.

        Returns:
            The result of the Lua script execution.

        Raises:
            RuntimeError: If the script cannot be registered after the retry.
        """
        if script_name not in self._script_shas:
            await self._register_script(script_name)

        sha = self._script_shas[script_name]
        try:
            return await cast(Awaitable, self.redis.evalsha(sha, num_keys, *args))
        except redis.exceptions.NoScriptError:
            logger.warning(
                "Lua script cache miss; reloading (async): limiter=%s, script=%s.",
                self.id,
                script_name,
            )
            await self._register_script(script_name)
            new_sha = self._script_shas[script_name]
            try:
                return await cast(
                    Awaitable, self.redis.evalsha(new_sha, num_keys, *args)
                )
            except redis.exceptions.NoScriptError:
                raise RuntimeError(
                    f"Redis failed to retain the Lua script '{script_name}' after reload."
                )

    async def start(self) -> None:
        """Perform async initialization that cannot occur in ``__init__``.

        Subclasses should override this method and call ``await super().start()``
        to register their required Lua scripts and start background tasks.
        """
        pass
