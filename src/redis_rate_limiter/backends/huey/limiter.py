from __future__ import annotations

import logging
from typing import Any, ClassVar, Optional, cast

from huey.api import Huey
from redis import Redis

from redis_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    SyncManagedRateLimiter,
)
from redis_rate_limiter.core.limiters import build_enhanced_payload

logger = logging.getLogger(__name__)


# noinspection PyUnnecessaryCast
class HueyRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
    """A rate limiter that dispatches tasks via the Huey task queue.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.

    ``max_concurrency`` should match the total number of Huey consumer workers
    (i.e., the ``--workers`` flag passed to the Huey consumer CLI). Dispatched
    tasks hold a concurrency lease while they wait in the task queue. If
    ``max_concurrency`` exceeds the actual consumer capacity, tasks accumulate
    in the queue, their leases expire, and the drain loop dispatches
    replacements, leading to unbounded queue growth.
    """

    _huey: ClassVar[Optional[Huey]] = None

    # ---------------------------------------------------------------------------
    # Managed backend hooks
    # ---------------------------------------------------------------------------

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure the shared Redis client and Huey instance context for the class-level API.

        Args:
            redis_client: The Redis client instance used for rate limiting state.
            **backend_context: Backend-specific keyword arguments. The ``huey``
                keyword argument is required
                (e.g., ``HueyRateLimiter.configure(redis, huey=huey_instance)``).
        """
        super().configure(redis_client, **backend_context)

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by Huey-backed limiter instances.

        Raises:
            RuntimeError: If the ``huey`` keyword argument is not provided.
        """
        huey = backend_context.get("huey")
        if huey is None:
            # fmt: off
            raise RuntimeError(
                "HueyRateLimiter.configure(redis_client, huey=huey_instance) must be called before create() or get()."
            )
            # fmt: on
        cls._huey = huey

        from redis_rate_limiter.backends.huey.tasks.worker import register_worker

        register_worker(huey)

    @classmethod
    def _has_backend_context(cls) -> bool:
        return cls._huey is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        assert cls._huey is not None
        return {"huey": cls._huey}

    @classmethod
    def _reset_backend_context(cls) -> None:
        cls._huey = None

    @classmethod
    def _configure_hint(cls) -> str:
        return "HueyRateLimiter.configure(redis_client, huey=huey_instance)"

    # ---------------------------------------------------------------------------
    # Instance construction
    # ---------------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        huey: Huey,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct a Huey rate limiter instance through the managed class API.

        Args:
            redis_client: The Redis client used for state management.
            huey: The ``Huey`` instance used for task dispatch.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, _sentinel=_sentinel, **kwargs)
        self.huey = huey

    # ---------------------------------------------------------------------------
    # Backend dispatch
    # ---------------------------------------------------------------------------

    def schedule_task(
        self,
        func_path: str,
        payload: dict,
        priority: int = 100,
        max_age: Optional[int] = None,
        use_executor: bool = True,
    ) -> tuple[bool, str]:
        enhanced_payload = build_enhanced_payload(payload, use_executor)

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
            from redis_rate_limiter.backends.huey.tasks.worker import (
                generic_rate_limited_worker,
            )

            generic_rate_limited_worker(
                limiter_id=self.id,
                func_path=func_path,
                payload=data,
                _rate_limit_task_id=task_id,
            )
            logger.debug(
                "[HueyRateLimiter] Task sent to generic worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
        else:
            from redis_rate_limiter.core.importing import import_string

            task_func = import_string(func_path)
            task_func(data, _rate_limit_task_id=task_id)
            logger.debug(
                "[HueyRateLimiter] Task sent to custom worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
