"""Managed rate limiter mixin and sync/async implementations.

This module provides a singleton-style management layer that handles instance
caching, Redis-backed configuration persistence, and cross-process version-based
configuration refresh. The pure Python logic lives in ``ManagedRateLimiterMixin``;
Redis I/O is provided by ``SyncManagedRateLimiter`` (for blocking Redis) and
``AsyncManagedRateLimiter`` (for ``redis.asyncio``).

Concrete backends compose these classes with the appropriate distributed or
request-oriented base via multiple inheritance:

- ``CeleryRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter)``
- ``AsyncIOTaskLimiter(AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter)``
- ``ASGIRateLimiter(AsyncManagedRateLimiter, AbstractAsyncRateLimiter)``
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Awaitable, ClassVar, Dict, Optional, cast

from redis import Redis

if TYPE_CHECKING:
    from redis.asyncio import Redis as AsyncRedis

logger = logging.getLogger(__name__)


class ManagedRateLimiterMixin:
    """Pure Python logic for singleton-style managed rate limiters.

    Rate limiter configuration is persisted to Redis as a single source of truth. All workers sharing the same limiter ID read from this authoritative store, and configuration updates (via ``update()``) are propagated automatically through a Redis-backed version counter that each instance polls via ``refresh_config()``.

    The singleton-per-identifier constraint ensures that each process holds exactly one canonical instance for a given limiter ID, so that a single ``refresh_config()`` call updates the one authoritative local object. It also guards against accidental overwrites: ``create()`` rejects duplicate identifiers unless ``override=True`` is explicitly passed.

    Provides instance caching, sentinel-based construction guard, abstract methods for backend-specific context, and static helpers for parsing Redis-stored configuration and version values. All Redis I/O is delegated to the sync or async subclass.

    Each concrete subclass receives isolated class-level state via ``__init_subclass__``, preventing cross-class pollution of the instance cache and sentinel.
    """

    _REGISTRY_KEY: ClassVar[str] = "rl:registry:configs"
    _VERSION_KEY: ClassVar[str] = "rl:registry:versions"
    _SENTINEL: ClassVar[object] = object()
    _redis_client: ClassVar[Optional[Any]] = None
    _instances: ClassVar[Dict[str, Any]] = {}

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Ensure that each subclass receives isolated class-level state.

        The explicit ``@classmethod`` decorator is redundant at runtime
        (Python implicitly wraps ``__init_subclass__``), but it prevents
        mutmut's trampoline from rewriting the first parameter as ``self``
        instead of ``cls``, which would cause an ``AttributeError`` during
        class construction and poison the entire test collection.
        See: https://github.com/boxed/mutmut/issues/366
        """
        super().__init_subclass__(**kwargs)
        cls._SENTINEL = object()
        cls._redis_client = None
        cls._instances = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Cooperative ``__init__`` that forwards all arguments through the MRO."""
        super().__init__(*args, **kwargs)

    # ---------------------------------------------------------------------------
    # Abstract backend hooks
    # ---------------------------------------------------------------------------

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific class context during ``configure()``."""
        raise NotImplementedError("Subclasses must implement _configure_backend")

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Determine whether the backend-specific class context has been configured."""
        raise NotImplementedError("Subclasses must implement _has_backend_context")

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Return the backend context to be forwarded to concrete instance constructors."""
        raise NotImplementedError("Subclasses must implement _get_instance_context")

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear the backend-specific class context, intended for testing and resets."""
        raise NotImplementedError("Subclasses must implement _reset_backend_context")

    @classmethod
    def _configure_hint(cls) -> str:
        """Return a human-readable ``configure()`` usage hint suitable for error messages."""
        raise NotImplementedError("Subclasses must implement _configure_hint")

    # ---------------------------------------------------------------------------
    # Construction guard and lifecycle helpers
    # ---------------------------------------------------------------------------

    @classmethod
    def _require_internal_construction(cls, sentinel: Any) -> None:
        """Reject direct constructor invocations that bypass the managed class API."""
        if sentinel is not cls._SENTINEL:
            raise RuntimeError(
                f"Direct {cls.__name__}() construction is not supported. "
                f"Use {cls._configure_hint()} then {cls.__name__}.create() or {cls.__name__}.get()."
            )

    @classmethod
    def _require_configured(cls) -> None:
        """Raise an error if ``configure()`` has not been called with the required context."""
        if cls._redis_client is None or not cls._has_backend_context():
            raise RuntimeError(
                f"{cls._configure_hint()} must be called before create() or get()."
            )

    @classmethod
    def _reset(cls) -> None:
        """Clear all class-level singleton state. This method is intended for use in tests."""
        cls._instances.clear()
        cls._redis_client = None
        cls._reset_backend_context()

    # ---------------------------------------------------------------------------
    # Static parsing helpers
    # ---------------------------------------------------------------------------

    @staticmethod
    def _parse_raw_config(raw_config: Any) -> dict[str, Any]:
        """Parse a raw Redis hash value into a configuration dictionary.

        Args:
            raw_config: The raw bytes or string value from ``HGET``.

        Returns:
            A dictionary of configuration key-value pairs.

        Raises:
            json.JSONDecodeError: If the stored value is not valid JSON.
            TypeError: If the value cannot be decoded.
        """
        return dict(
            json.loads(
                raw_config.decode("utf-8")  # pragma: no mutate
                if isinstance(raw_config, bytes)
                else str(raw_config)
            )
        )

    @staticmethod
    def _parse_version(raw_version: Any) -> int:
        """Parse a raw Redis hash value into an integer version number.

        Args:
            raw_version: The raw bytes, string, or integer value from ``HGET``.

        Returns:
            The integer version number.
        """
        # This cast is necessary for mypy type validation.
        # fmt: off
        version_value = cast(  # pragma: no mutate
            str | bytes | int, raw_version
        )
        # fmt: on
        return int(
            version_value.decode("utf-8")
            if isinstance(version_value, bytes)
            else version_value
        )


# noinspection PyUnnecessaryCast
class SyncManagedRateLimiter(ManagedRateLimiterMixin):
    """Synchronous Redis-backed managed rate limiter.

    Provides ``configure``, ``create``, ``get``, ``update``, and ``refresh_config``
    using blocking ``redis.Redis`` operations.
    """

    if TYPE_CHECKING:
        id: str
        redis: Redis
        _config_version: int

        def _apply_config_overrides(self, overrides: dict[str, Any]) -> None: ...
        def _build_persist_config(self) -> dict[str, Any]: ...

    _redis_client: ClassVar[Optional[Redis]] = None

    # ---------------------------------------------------------------------------
    # Managed class API
    # ---------------------------------------------------------------------------

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure the shared Redis client and backend context for class-level API usage."""
        cls._redis_client = redis_client
        cls._configure_backend(**backend_context)
        logger.info("%s configured.", cls.__name__)

    def __init__(
        self,
        redis_client: Redis,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ) -> None:
        """Construct a managed limiter instance via internal class API flows."""
        self.__class__._require_internal_construction(_sentinel)
        super().__init__(redis_client, **kwargs)

    @classmethod
    def create(
        cls,
        limiter_id: str,
        *,
        persist: bool = True,
        override: bool = False,
        **config: Any,
    ) -> "SyncManagedRateLimiter":
        """Create and cache a limiter instance, optionally persisting the configuration to Redis.

        Args:
            limiter_id: The unique identifier of the rate limiter to create.
            persist: Whether to persist the configuration to Redis (default: ``True``).
            override: Whether to replace an existing instance with the same identifier.
            **config: All limiter configuration parameters (e.g., ``limit``, ``window``,
                ``max_concurrency``). These are forwarded to the concrete constructor.

        Returns:
            The newly created and cached limiter instance.

        Raises:
            ValueError: If an instance with the specified identifier already exists and
                ``override`` is ``False``.
            RuntimeError: If ``configure()`` has not been called.
        """
        cls._require_configured()

        if not override and limiter_id in cls._instances:
            raise ValueError(
                f"Limiter '{limiter_id}' already exists. Use override=True to replace it."
            )

        assert cls._redis_client is not None
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            _sentinel=cls._SENTINEL,
            **cls._get_instance_context(),
            **config,
        )
        cls._instances[limiter_id] = instance

        if persist:
            cls._persist_config(instance)

        logger.info(
            "%s created: limiter=%s, config=%s, persist=%s.",
            cls.__name__,
            limiter_id,
            config,
            persist,
        )
        return instance

    @classmethod
    def get(cls, limiter_id: str) -> "SyncManagedRateLimiter":
        """Retrieve a limiter by its identifier from the local cache or the Redis registry.

        Args:
            limiter_id: The unique identifier of the limiter to retrieve.

        Returns:
            The cached or hydrated limiter instance.

        Raises:
            ValueError: If the limiter is not found in the local cache or Redis.
            RuntimeError: If ``configure()`` has not been called.
        """
        if limiter_id in cls._instances:
            logger.debug(
                "%s resolved from local cache: limiter=%s.",
                cls.__name__,
                limiter_id,
            )
            # fmt: off
            return cast(  # pragma: no mutate
                SyncManagedRateLimiter, cls._instances[limiter_id]
            )
            # fmt: on

        cls._require_configured()
        assert cls._redis_client is not None

        raw_config = cls._redis_client.hget(cls._REGISTRY_KEY, limiter_id)
        if raw_config is None:
            raise ValueError(
                f"Limiter '{limiter_id}' not found in local cache or Redis. "
                f"Ensure it was created via {cls.__name__}.create()."
            )

        config = cls._parse_raw_config(raw_config)
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            _sentinel=cls._SENTINEL,
            **cls._get_instance_context(),
            **config,
        )

        raw_version = cls._redis_client.hget(cls._VERSION_KEY, limiter_id)
        if raw_version is not None:
            instance._config_version = cls._parse_version(raw_version)

        cls._instances[limiter_id] = instance
        logger.debug(
            "%s hydrated from Redis: limiter=%s.",
            cls.__name__,
            limiter_id,
        )
        return instance

    @classmethod
    def update(
        cls,
        limiter_id: str,
        **overrides: Any,
    ) -> "SyncManagedRateLimiter":
        """Update the limiter configuration, persist it to Redis, and increment the version counter.

        Args:
            limiter_id: The unique identifier of the limiter to update.
            **overrides: The configuration fields to change (e.g., ``limit=10``,
                ``window=30.0``).

        Returns:
            The updated limiter instance.
        """
        instance = cls.get(limiter_id)
        instance._apply_config_overrides(overrides)
        cls._persist_config(instance)

        logger.info(
            "%s updated: limiter=%s, overrides=%s.",
            cls.__name__,
            limiter_id,
            overrides,
        )
        return instance

    # ---------------------------------------------------------------------------
    # Config persistence
    # ---------------------------------------------------------------------------

    @classmethod
    def _persist_config(cls, instance: "SyncManagedRateLimiter") -> None:
        """Write the limiter configuration to Redis and increment its version counter."""
        assert cls._redis_client is not None
        config = instance._build_persist_config()
        cls._redis_client.hset(cls._REGISTRY_KEY, instance.id, json.dumps(config))
        cls._redis_client.hincrby(cls._VERSION_KEY, instance.id, 1)

        raw_version = cls._redis_client.hget(cls._VERSION_KEY, instance.id)
        if raw_version is not None:
            instance._config_version = cls._parse_version(raw_version)

    def refresh_config(self) -> bool:
        """Apply a newer persisted configuration from Redis when a version change is detected.

        Returns:
            ``True`` if the configuration was updated, ``False`` otherwise.
        """
        raw_version = self.redis.hget(self.__class__._VERSION_KEY, self.id)
        if raw_version is None:
            return False

        remote_version = self._parse_version(raw_version)
        if remote_version <= self._config_version:
            return False

        raw_config = self.redis.hget(self.__class__._REGISTRY_KEY, self.id)
        if raw_config is None:
            return False

        try:
            config = self._parse_raw_config(raw_config)
        except (json.JSONDecodeError, TypeError) as error:
            logger.warning(
                "Config refresh skipped due to malformed persisted config: limiter=%s, error=%s.",
                self.id,
                error,
            )
            return False

        self._apply_config_overrides(config)
        self._config_version = remote_version
        logger.info(
            "Config refreshed: limiter=%s, version=%d.",
            self.id,
            remote_version,
        )
        return True


# noinspection PyUnnecessaryCast
class AsyncManagedRateLimiter(ManagedRateLimiterMixin):
    """Asynchronous Redis-backed managed rate limiter.

    Provides ``configure``, ``create``, ``get``, ``update``, and ``refresh_config``
    using non-blocking ``redis.asyncio.Redis`` operations. The ``create`` and ``get``
    methods call ``await instance.start()`` after construction to handle deferred
    script registration and subscriber startup.
    """

    if TYPE_CHECKING:
        id: str
        redis: Any  # AsyncRedis; using Any to avoid shadowing the module name.
        _config_version: int

        def _apply_config_overrides(self, overrides: dict[str, Any]) -> None: ...
        def _build_persist_config(self) -> dict[str, Any]: ...
        async def start(self) -> None: ...

    _redis_client: ClassVar[Optional[AsyncRedis]] = None

    # ---------------------------------------------------------------------------
    # Managed class API
    # ---------------------------------------------------------------------------

    @classmethod
    def configure(cls, redis_client: AsyncRedis, **backend_context: Any) -> None:
        """Configure the shared async Redis client and backend context for class-level API usage."""
        cls._redis_client = redis_client
        cls._configure_backend(**backend_context)
        logger.info("%s configured.", cls.__name__)

    def __init__(
        self,
        redis_client: AsyncRedis,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ) -> None:
        """Construct a managed limiter instance via internal class API flows."""
        self.__class__._require_internal_construction(_sentinel)
        super().__init__(redis_client, **kwargs)

    @classmethod
    async def create(
        cls,
        limiter_id: str,
        *,
        persist: bool = True,
        override: bool = False,
        **config: Any,
    ) -> "AsyncManagedRateLimiter":
        """Create and cache a limiter instance, optionally persisting the configuration to Redis.

        After construction, ``await instance.start()`` is called to perform deferred
        async initialization (e.g., Lua script registration, subscriber startup).

        Args:
            limiter_id: The unique identifier of the rate limiter to create.
            persist: Whether to persist the configuration to Redis (default: ``True``).
            override: Whether to replace an existing instance with the same identifier.
            **config: All limiter configuration parameters.

        Returns:
            The newly created, started, and cached limiter instance.

        Raises:
            ValueError: If an instance with the specified identifier already exists and
                ``override`` is ``False``.
            RuntimeError: If ``configure()`` has not been called.
        """
        cls._require_configured()

        if not override and limiter_id in cls._instances:
            raise ValueError(
                f"Limiter '{limiter_id}' already exists. Use override=True to replace it."
            )

        assert cls._redis_client is not None
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            _sentinel=cls._SENTINEL,
            **cls._get_instance_context(),
            **config,
        )
        await instance.start()
        cls._instances[limiter_id] = instance

        if persist:
            await cls._persist_config(instance)

        logger.info(
            "%s created: limiter=%s, config=%s, persist=%s.",
            cls.__name__,
            limiter_id,
            config,
            persist,
        )
        return instance

    @classmethod
    async def get(cls, limiter_id: str) -> "AsyncManagedRateLimiter":
        """Retrieve a limiter by its identifier from the local cache or the Redis registry.

        When hydrating from Redis, ``await instance.start()`` is called to perform
        deferred async initialization.

        Args:
            limiter_id: The unique identifier of the limiter to retrieve.

        Returns:
            The cached or hydrated limiter instance.

        Raises:
            ValueError: If the limiter is not found in the local cache or Redis.
            RuntimeError: If ``configure()`` has not been called.
        """
        if limiter_id in cls._instances:
            logger.debug(
                "%s resolved from local cache: limiter=%s.",
                cls.__name__,
                limiter_id,
            )
            # fmt: off
            return cast(  # pragma: no mutate
                AsyncManagedRateLimiter, cls._instances[limiter_id]
            )
            # fmt: on

        cls._require_configured()
        assert cls._redis_client is not None

        # fmt: off
        raw_config = await cast(  # pragma: no mutate
            Awaitable, cls._redis_client.hget(cls._REGISTRY_KEY, limiter_id)
        )
        # fmt: on
        if raw_config is None:
            raise ValueError(
                f"Limiter '{limiter_id}' not found in local cache or Redis. "
                f"Ensure it was created via {cls.__name__}.create()."
            )

        config = cls._parse_raw_config(raw_config)
        instance = cls(
            redis_client=cls._redis_client,
            limiter_id=limiter_id,
            _sentinel=cls._SENTINEL,
            **cls._get_instance_context(),
            **config,
        )
        await instance.start()

        # fmt: off
        raw_version = await cast(  # pragma: no mutate
            Awaitable, cls._redis_client.hget(cls._VERSION_KEY, limiter_id)
        )
        # fmt: on
        if raw_version is not None:
            instance._config_version = cls._parse_version(raw_version)

        cls._instances[limiter_id] = instance
        logger.debug(
            "%s hydrated from Redis: limiter=%s.",
            cls.__name__,
            limiter_id,
        )
        return instance

    @classmethod
    async def update(
        cls,
        limiter_id: str,
        **overrides: Any,
    ) -> "AsyncManagedRateLimiter":
        """Update the limiter configuration, persist it to Redis, and increment the version counter.

        Args:
            limiter_id: The unique identifier of the limiter to update.
            **overrides: The configuration fields to change.

        Returns:
            The updated limiter instance.
        """
        instance = await cls.get(limiter_id)
        instance._apply_config_overrides(overrides)
        await cls._persist_config(instance)

        logger.info(
            "%s updated: limiter=%s, overrides=%s.",
            cls.__name__,
            limiter_id,
            overrides,
        )
        return instance

    # ---------------------------------------------------------------------------
    # Config persistence
    # ---------------------------------------------------------------------------

    @classmethod
    async def _persist_config(cls, instance: "AsyncManagedRateLimiter") -> None:
        """Write the limiter configuration to Redis and increment its version counter."""
        assert cls._redis_client is not None
        config = instance._build_persist_config()

        # fmt: off
        await cast(  # pragma: no mutate
            Awaitable,
            cls._redis_client.hset(cls._REGISTRY_KEY, instance.id, json.dumps(config)),
        )
        await cast(  # pragma: no mutate
            Awaitable, cls._redis_client.hincrby(cls._VERSION_KEY, instance.id, 1)
        )

        raw_version = await cast(  # pragma: no mutate
            Awaitable, cls._redis_client.hget(cls._VERSION_KEY, instance.id)
        )
        # fmt: on
        if raw_version is not None:
            instance._config_version = cls._parse_version(raw_version)

    async def refresh_config(self) -> bool:
        """Apply a newer persisted configuration from Redis when a version change is detected.

        Returns:
            ``True`` if the configuration was updated, ``False`` otherwise.
        """
        raw_version = await self.redis.hget(self.__class__._VERSION_KEY, self.id)
        if raw_version is None:
            return False

        remote_version = self._parse_version(raw_version)
        if remote_version <= self._config_version:
            return False

        raw_config = await self.redis.hget(self.__class__._REGISTRY_KEY, self.id)
        if raw_config is None:
            return False

        try:
            config = self._parse_raw_config(raw_config)
        except (json.JSONDecodeError, TypeError) as error:
            logger.warning(
                "Config refresh skipped due to malformed persisted config: limiter=%s, error=%s.",
                self.id,
                error,
            )
            return False

        self._apply_config_overrides(config)
        self._config_version = remote_version
        logger.info(
            "Config refreshed (async): limiter=%s, version=%d.",
            self.id,
            remote_version,
        )
        return True
