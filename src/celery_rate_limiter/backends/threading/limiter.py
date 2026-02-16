from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar, Optional

from redis import Redis

from celery_rate_limiter.core import AbstractRedisManagedRateLimiter, import_string

logger = logging.getLogger(__name__)


class ThreadPoolRateLimiter(AbstractRedisManagedRateLimiter):
    """A rate limiter that dispatches tasks to a local thread pool.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.
    """

    _executor: ClassVar[Optional[ThreadPoolExecutor]] = None

    # No backend-specific ``configure`` override is required; the base class implementation suffices.

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by thread pool limiter instances."""
        executor = backend_context.get("executor")
        if executor is None:
            raise RuntimeError(
                "ThreadPoolRateLimiter.configure(redis_client, executor=executor) "
                "must be called before create() or get()."
            )
        cls._executor = executor

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Determine whether the executor context has been configured."""
        return cls._executor is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Provide the constructor context required for concrete instance creation."""
        assert cls._executor is not None
        return {"executor": cls._executor}

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear the executor context held at the class level."""
        cls._executor = None

    @classmethod
    def _configure_hint(cls) -> str:
        """Return the ``configure`` usage hint to be included in runtime error messages."""
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
        """Construct a thread pool rate limiter instance through the managed class API.

        Args:
            redis_client: The Redis client used for state management.
            executor: The ``ThreadPoolExecutor`` instance used for task dispatch.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, *args, _sentinel=_sentinel, **kwargs)
        self.executor = executor
        self._local_dispatched: int = 0
        self._local_dispatch_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Local capacity guard
    # ------------------------------------------------------------------

    def _has_local_capacity(self) -> bool:
        """Check whether the local thread pool can accept another task.

        Returns ``False`` when the number of dispatched-but-not-yet-completed
        tasks equals the executor's ``max_workers``. This prevents acquiring
        Redis concurrency slots for tasks that would only be queued locally
        in the thread pool.
        """
        with self._local_dispatch_lock:
            return self._local_dispatched < self.executor._max_workers

    # ------------------------------------------------------------------
    # Backend dispatch
    # ------------------------------------------------------------------

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        target_func = import_string(func_path)

        with self._local_dispatch_lock:
            self._local_dispatched += 1

        def _run_task() -> None:
            try:
                with self.task_lifecycle(task_id):
                    target_func(**payload)
            finally:
                with self._local_dispatch_lock:
                    self._local_dispatched -= 1

        self.executor.submit(_run_task)
        logger.debug(
            "Task submitted to thread pool: limiter=%s, task_id=%s, func_path=%s, local_dispatched=%d.",
            self.id,
            task_id,
            func_path,
            self._local_dispatched,
        )
