"""AsyncIO task limiter backend.

The async counterpart of ``ThreadPoolRateLimiter``. Dispatches rate-limited
tasks as ``asyncio.Task`` instances within the current event loop, with a
full async drain loop, Pub/Sub subscriber, and task lifecycle heartbeat.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar, Optional

import redis.asyncio

from celery_rate_limiter.core.async_limiters import AbstractAsyncDistributedRateLimiter
from celery_rate_limiter.core.importing import import_string
from celery_rate_limiter.core.managed import AsyncManagedRateLimiter

logger = logging.getLogger(__name__)


class AsyncIOTaskLimiter(AsyncManagedRateLimiter, AbstractAsyncDistributedRateLimiter):
    """A rate limiter that dispatches tasks as ``asyncio.Task`` instances.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.
    """

    _max_tasks: ClassVar[Optional[int]] = None

    # ------------------------------------------------------------------
    # Managed backend hooks
    # ------------------------------------------------------------------

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by async task limiter instances."""
        max_tasks = backend_context.get("max_tasks")
        if max_tasks is None:
            raise RuntimeError(
                "AsyncIOTaskLimiter.configure(redis_client, max_tasks=N) "
                "must be called before create() or get()."
            )
        cls._max_tasks = int(max_tasks)

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Determine whether the max_tasks context has been configured."""
        return cls._max_tasks is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Provide the constructor context required for concrete instance creation."""
        assert cls._max_tasks is not None
        return {"max_tasks": cls._max_tasks}

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear the max_tasks context held at the class level."""
        cls._max_tasks = None

    @classmethod
    def _configure_hint(cls) -> str:
        """Return the ``configure`` usage hint for runtime error messages."""
        return "AsyncIOTaskLimiter.configure(redis_client, max_tasks=N)"

    # ------------------------------------------------------------------
    # Instance construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        redis_client: redis.asyncio.Redis,
        max_tasks: int,
        *args: Any,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct an async task limiter instance through the managed class API.

        Args:
            redis_client: The async Redis client used for state management.
            max_tasks: The maximum number of concurrent asyncio tasks.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from
        ``AbstractAsyncDistributedRateLimiter``.
        """
        super().__init__(redis_client, *args, _sentinel=_sentinel, **kwargs)
        self.max_tasks = max_tasks
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._active_count: int = 0

    # ------------------------------------------------------------------
    # Local capacity guard
    # ------------------------------------------------------------------

    def _has_local_capacity(self) -> bool:
        """Check whether the local event loop can accept another task.

        Returns ``False`` when the number of dispatched-but-not-yet-completed
        tasks equals ``max_tasks``.
        """
        return self._active_count < self.max_tasks

    # ------------------------------------------------------------------
    # Backend dispatch
    # ------------------------------------------------------------------

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Dispatch a task as an ``asyncio.Task`` within the current event loop."""
        target_func = import_string(func_path)

        self._active_count += 1

        async def _run_task() -> None:
            try:
                async with self.task_lifecycle(task_id):
                    if not asyncio.iscoroutinefunction(target_func):
                        raise TypeError(
                            f"AsyncIOTaskLimiter requires coroutine functions, "
                            f"but '{func_path}' is synchronous. Define it with "
                            f"'async def' or use ThreadPoolRateLimiter instead."
                        )
                    await target_func(**payload)
            except Exception:
                logger.exception(
                    "Task raised an exception: limiter=%s, task_id=%s, func_path=%s.",
                    self.id,
                    task_id,
                    func_path,
                )
            finally:
                self._active_count -= 1
                self._active_tasks.discard(task)

        task = asyncio.create_task(_run_task())
        self._active_tasks.add(task)

        logger.debug(
            "Task submitted to event loop: limiter=%s, task_id=%s, func_path=%s, active_count=%d.",
            self.id,
            task_id,
            func_path,
            self._active_count,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def shutdown(self) -> None:
        """Stop the drain loop, signal subscriber, and cancel all active tasks."""
        await super().shutdown()

        if self._active_tasks:
            logger.info(
                "Cancelling %d active tasks: limiter=%s.",
                len(self._active_tasks),
                self.id,
            )
            for task in list(self._active_tasks):
                task.cancel()
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
            self._active_tasks.clear()
            self._active_count = 0
