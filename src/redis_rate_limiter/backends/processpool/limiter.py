from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any, ClassVar, Optional

from redis import Redis

from redis_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    SyncManagedRateLimiter,
    import_string,
)

logger = logging.getLogger(__name__)


class ProcessPoolRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
    """A rate limiter that dispatches tasks to a local process pool.

    Unlike the thread pool variant, task functions and their payloads must be
    picklable because they are serialized and sent to child processes. The task
    lifecycle (heartbeat, lease management) is managed in the parent process
    via a ``Future`` done callback rather than within the child process.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.
    """

    _executor: ClassVar[Optional[ProcessPoolExecutor]] = None

    # ---------------------------------------------------------------------------
    # Managed backend hooks
    # ---------------------------------------------------------------------------

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by process pool limiter instances.

        Raises:
            RuntimeError: If the ``executor`` keyword argument is not provided.
        """
        executor = backend_context.get("executor")
        if executor is None:
            raise RuntimeError(
                "ProcessPoolRateLimiter.configure(redis_client, executor=executor) "
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
        return "ProcessPoolRateLimiter.configure(redis_client, executor=executor)"

    # ---------------------------------------------------------------------------
    # Instance construction
    # ---------------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        executor: ProcessPoolExecutor,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct a process pool rate limiter instance through the managed class API.

        Args:
            redis_client: The Redis client used for state management.
            executor: The ``ProcessPoolExecutor`` instance used for task dispatch.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, _sentinel=_sentinel, **kwargs)
        self.executor = executor
        self._local_max_workers: int = getattr(executor, "_max_workers")
        self._local_dispatched: int = 0
        self._local_dispatch_lock = threading.Lock()

    # ---------------------------------------------------------------------------
    # Local capacity guard
    # ---------------------------------------------------------------------------

    def _has_local_capacity(self) -> bool:
        """Check whether the local process pool can accept another task.

        Returns ``False`` when the number of dispatched-but-not-yet-completed
        tasks equals the executor's ``max_workers``. This prevents acquiring
        Redis concurrency slots for tasks that would only be queued locally
        in the process pool.
        """
        with self._local_dispatch_lock:
            return self._local_dispatched < self._local_max_workers

    # ---------------------------------------------------------------------------
    # Backend dispatch
    # ---------------------------------------------------------------------------

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        target_func = import_string(func_path)

        with self._local_dispatch_lock:
            self._local_dispatched += 1

        lifecycle = self.task_lifecycle(task_id)
        lifecycle.__enter__()

        future = self.executor.submit(target_func, **payload)

        def _on_done(f: Future[Any]) -> None:
            try:
                lifecycle.__exit__(None, None, None)
            finally:
                with self._local_dispatch_lock:
                    self._local_dispatched -= 1

        future.add_done_callback(_on_done)
        logger.debug(
            "Task submitted to process pool: limiter=%s, task_id=%s, func_path=%s, local_dispatched=%d.",
            self.id,
            task_id,
            func_path,
            self._local_dispatched,
        )
