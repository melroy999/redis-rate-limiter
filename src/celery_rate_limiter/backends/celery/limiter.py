from __future__ import annotations

import logging
from typing import Any, ClassVar, Optional, cast

from celery import Celery
from redis import Redis

from celery_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    SyncManagedRateLimiter,
)

logger = logging.getLogger(__name__)


# noinspection PyUnnecessaryCast
class CeleryRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
    """A rate limiter that dispatches tasks via the Celery distributed task queue.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.
    """

    _celery_app: ClassVar[Optional[Celery]] = None

    # ------------------------------------------------------------------
    # Managed backend hooks
    # ------------------------------------------------------------------

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure the shared Redis client and Celery application context for the class-level API.

        The ``celery_app`` keyword argument is required
        (e.g., ``CeleryRateLimiter.configure(redis, celery_app=app)``).
        """
        super().configure(redis_client, **backend_context)

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by Celery-backed limiter instances."""
        celery_app = backend_context.get("celery_app")
        if celery_app is None:
            raise RuntimeError(
                "CeleryRateLimiter.configure(redis_client, celery_app) "
                "must be called before create() or get()."
            )
        cls._celery_app = celery_app

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Determine whether the Celery application context has been configured."""
        return cls._celery_app is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Provide the constructor context required for concrete instance creation."""
        assert cls._celery_app is not None
        return {"celery_app": cls._celery_app}

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear the Celery application context held at the class level."""
        cls._celery_app = None

    @classmethod
    def _configure_hint(cls) -> str:
        """Return the ``configure`` usage hint to be included in runtime error messages."""
        return "CeleryRateLimiter.configure(redis_client, celery_app)"

    # ------------------------------------------------------------------
    # Instance construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        celery_app: Celery,
        *args: Any,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct a Celery rate limiter instance through the managed class API.

        Args:
            redis_client: The Redis client used for state management.
            celery_app: The Celery application instance used for task dispatch.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, *args, _sentinel=_sentinel, **kwargs)
        self.app = celery_app

    # ------------------------------------------------------------------
    # Backend dispatch
    # ------------------------------------------------------------------

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
        # Augment the payload with the executor flag.
        enhanced_payload = self._get_enhanced_payload(payload, use_executor)

        # Delegate to the parent scheduler.
        return cast(  # pragma: no mutate
            tuple[bool, str],
            super().schedule_task(func_path, enhanced_payload, priority, max_age),
        )

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        # Determine whether the built-in generic worker should be used.
        use_executor = payload.get("meta", {}).get("use_executor", True)
        data = payload.get("data", {})

        if use_executor:
            # Dispatch the task to the generic worker task.
            self.app.send_task(
                "celery_rate_limiter.generic_worker",
                kwargs={
                    "limiter_id": self.id,
                    "func_path": func_path,
                    "payload": data,
                    "_rate_limit_task_id": task_id,
                },
            )
            logger.debug(
                "Celery task sent to generic worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
        else:
            # Dispatch to the user-defined custom task.
            self.app.send_task(
                func_path, args=[data], kwargs={"_rate_limit_task_id": task_id}
            )
            logger.debug(
                "Celery task sent to custom worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
