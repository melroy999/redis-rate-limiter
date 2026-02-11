from __future__ import annotations

import logging
import warnings
from typing import Any, ClassVar, Optional, cast

from celery import Celery
from redis import Redis

from celery_rate_limiter.core.limiters import AbstractRedisManagedRateLimiter

logger = logging.getLogger(__name__)


class CeleryRateLimiter(AbstractRedisManagedRateLimiter):
    """A rate limiter that dispatches tasks via Celery.

    Use the classmethods ``configure``, ``create``, ``get``, and ``update``
    instead of constructing instances directly.
    """

    _celery_app: ClassVar[Optional[Celery]] = None

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure shared Redis and Celery app context for class API usage.

        Requires ``celery_app`` as a keyword argument
        (e.g. ``CeleryRateLimiter.configure(redis, celery_app=app)``).
        """
        super().configure(redis_client, **backend_context)

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store backend-specific context for Celery-backed limiter instances."""
        celery_app = backend_context.get("celery_app")
        if celery_app is None:
            raise RuntimeError(
                "CeleryRateLimiter.configure(redis_client, celery_app) "
                "must be called before create() or get()."
            )
        cls._celery_app = celery_app

    @classmethod
    def _has_backend_context(cls) -> bool:
        """Check if Celery app context has been configured."""
        return cls._celery_app is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        """Expose constructor context for concrete instance creation."""
        assert cls._celery_app is not None
        return {"celery_app": cls._celery_app, "_sentinel": cls._SENTINEL}

    @classmethod
    def _reset_backend_context(cls) -> None:
        """Clear Celery app class context."""
        cls._celery_app = None

    @classmethod
    def _configure_hint(cls) -> str:
        """Return configure usage for runtime errors."""
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
        """Create a Celery rate limiter instance.

        Deprecated:
            Direct construction is deprecated.  Use ``CeleryRateLimiter.create()`` or
            ``CeleryRateLimiter.get()`` instead.

        Args:
            redis_client: The Redis client.
            celery_app: The Celery app to use for task dispatch.
            _sentinel: Internal — passed by classmethods to suppress the deprecation warning.

        Other parameters are inherited from AbstractDistributedRateLimiter.
        """
        if _sentinel is not self.__class__._SENTINEL:
            warnings.warn(
                "Direct CeleryRateLimiter() construction is deprecated. "
                "Use CeleryRateLimiter.configure() + .create() or .get() instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(redis_client, *args, **kwargs)
        self.app = celery_app

    @staticmethod
    def _get_enhanced_payload(payload: dict, use_executor: bool) -> dict:
        """Get the enhanced payload with metadata.

        Args:
            payload: The original task payload.
            use_executor: Whether to use the generic executor.

        Returns:
            The enhanced payload with metadata.
        """
        return {"data": payload, "meta": {"use_executor": use_executor}}

    def schedule_task(
            self,
            func_path: str,
            payload: dict,
            priority: int = 100,
            max_age: Optional[int] = None,
            retry: bool = True,
            use_executor: bool = True,
    ) -> tuple[bool, str]:
        # Add the use executor flag to the payload.
        # Only add this if we aren't re-trying--the payload is already present otherwise.
        enhanced_payload = payload
        if retry:
            enhanced_payload = self._get_enhanced_payload(payload, use_executor)

        # Call the parent scheduler.
        # noinspection PyUnnecessaryCast
        # This cast is in fact necessary for mypy validation.
        return cast(
            tuple[bool, str],
            super().schedule_task(func_path, enhanced_payload, priority, max_age, retry),
        )

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        # Check if the built-in worker should be used.
        use_executor = payload.get("meta", {}).get("use_executor", True)
        data = payload.get("data", {})

        if use_executor:
            # Dispatch the task to the generic worker.
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
            # Use the custom user task.
            self.app.send_task(
                func_path, args=[data], kwargs={"_rate_limit_task_id": task_id}
            )
            logger.debug(
                "Celery task sent to custom worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )

    def _schedule_drain(self, delay: float = 0.0) -> None:
        # Schedule an attempt at consuming a token.
        self.app.send_task(
            "celery_rate_limiter.attempt_consume", args=[self.id], countdown=delay
        )
        logger.debug(
            "Drain scheduled via Celery: limiter=%s, countdown_s=%.3f.",
            self.id,
            delay,
        )
