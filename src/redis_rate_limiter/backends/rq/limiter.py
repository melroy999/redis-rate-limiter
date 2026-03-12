from __future__ import annotations

import logging
from typing import Any, ClassVar, Optional, cast

from redis import Redis
from rq import Queue

from redis_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    SyncManagedRateLimiter,
)

logger = logging.getLogger(__name__)


# noinspection PyUnnecessaryCast
class RQRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
    """A rate limiter that dispatches tasks via the RQ (Redis Queue) job queue.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.
    """

    _queue: ClassVar[Optional[Queue]] = None

    # ---------------------------------------------------------------------------
    # Managed backend hooks
    # ---------------------------------------------------------------------------

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure the shared Redis client and RQ queue context for the class-level API.

        Args:
            redis_client: The Redis client instance used for rate limiting state.
            **backend_context: Backend-specific keyword arguments. The ``queue``
                keyword argument is required
                (e.g., ``RQRateLimiter.configure(redis, queue=queue)``).
        """
        super().configure(redis_client, **backend_context)

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by RQ-backed limiter instances.

        Raises:
            RuntimeError: If the ``queue`` keyword argument is not provided.
        """
        queue = backend_context.get("queue")
        if queue is None:
            raise RuntimeError(
                "RQRateLimiter.configure(redis_client, queue=queue) "
                "must be called before create() or get()."
            )
        cls._queue = queue

    @classmethod
    def _has_backend_context(cls) -> bool:
        return cls._queue is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        assert cls._queue is not None
        return {"queue": cls._queue}

    @classmethod
    def _reset_backend_context(cls) -> None:
        cls._queue = None

    @classmethod
    def _configure_hint(cls) -> str:
        return "RQRateLimiter.configure(redis_client, queue=queue)"

    # ---------------------------------------------------------------------------
    # Instance construction
    # ---------------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        queue: Queue,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct an RQ rate limiter instance through the managed class API.

        Args:
            redis_client: The Redis client used for state management.
            queue: The RQ ``Queue`` instance used for job dispatch.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, _sentinel=_sentinel, **kwargs)
        self.queue = queue

    # ---------------------------------------------------------------------------
    # Backend health check
    # ---------------------------------------------------------------------------

    def _check_backend_health(self) -> bool:
        """Check whether at least one RQ worker is listening on the configured queue."""
        from rq import Worker

        workers = Worker.all(connection=self.queue.connection)
        return any(self.queue.name in w.queue_names() for w in workers)

    # ---------------------------------------------------------------------------
    # Backend dispatch
    # ---------------------------------------------------------------------------

    @staticmethod
    def _get_enhanced_payload(payload: dict, use_executor: bool) -> dict:
        """Produce an enhanced payload that includes dispatch metadata.

        Args:
            payload: The original task payload.
            use_executor: Whether the generic executor should be used for dispatch.

        Returns:
            A dictionary containing the original payload augmented with metadata.
        """
        return {"data": payload, "meta": {"use_executor": use_executor}}

    def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
        use_executor: bool = True,
    ) -> tuple[bool, str]:
        enhanced_payload = self._get_enhanced_payload(payload, use_executor)

        # fmt: off
        return cast(  # pragma: no mutate
            tuple[bool, str],
            super().schedule_task(func_path, enhanced_payload, priority, max_age),
        )
        # fmt: on

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        use_executor = payload.get("meta", {}).get("use_executor", True)
        data = payload.get("data", {})

        if use_executor:
            from redis_rate_limiter.backends.rq.tasks.worker import (
                generic_rate_limited_worker,
            )

            self.queue.enqueue(
                generic_rate_limited_worker,
                kwargs={
                    "limiter_id": self.id,
                    "func_path": func_path,
                    "payload": data,
                    "_rate_limit_task_id": task_id,
                },
            )
            logger.debug(
                "[RQRateLimiter] Job sent to generic worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
        else:
            self.queue.enqueue(
                func_path, args=[data], kwargs={"_rate_limit_task_id": task_id}
            )
            logger.debug(
                "[RQRateLimiter] Job sent to custom worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
