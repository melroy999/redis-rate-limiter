from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from typing import Any, ClassVar, Optional

from redis import Redis

from celery_rate_limiter.core import AbstractRedisManagedRateLimiter, import_string

logger = logging.getLogger(__name__)


class ThreadPoolRateLimiter(AbstractRedisManagedRateLimiter):
    """A rate limiter that dispatches tasks to a thread pool.

    Use the classmethods ``configure``, ``create``, ``get``, and ``update``
    instead of constructing instances directly.
    """

    _executor: ClassVar[Optional[ThreadPoolExecutor]] = None

    # No backend-specific configure() override needed; the base class handles it.

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store backend-specific context for thread pool limiter instances."""
        executor = backend_context.get("executor")
        if executor is None:
            raise RuntimeError(
                "ThreadPoolRateLimiter.configure(redis_client, executor=executor) "
                "must be called before create() or get()."
            )
        cls._executor = executor

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Check if executor context has been configured."""
        return cls._executor is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Expose constructor context for concrete instance creation."""
        assert cls._executor is not None
        return {"executor": cls._executor}

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear executor class context."""
        cls._executor = None

    @classmethod
    def _configure_hint(cls) -> str:
        """Return configure usage for runtime errors."""
        return "ThreadPoolRateLimiter.configure(redis_client, executor=executor)"

    # ------------------------------------------------------------------
    # Instance construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        executor: ThreadPoolExecutor,
        *args: Any,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Create a thread pool rate limiter instance via the managed class API.

        Args:
            redis_client: The Redis client.
            executor: The ThreadPoolExecutor to use for task dispatch.
            _sentinel: Internal sentinel passed by class API methods.

        Other parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, *args, _sentinel=_sentinel, **kwargs)
        self.executor = executor

    # ------------------------------------------------------------------
    # Backend dispatch
    # ------------------------------------------------------------------

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        target_func = import_string(func_path)

        def _run_task() -> None:
            with self.task_lifecycle(task_id):
                target_func(**payload)

        self.executor.submit(_run_task)
        logger.debug(
            "Task submitted to thread pool: limiter=%s, task_id=%s, func_path=%s.",
            self.id,
            task_id,
            func_path,
        )
