from __future__ import annotations

import logging
from typing import Any, ClassVar, Optional, cast

import dramatiq
from redis import Redis

from redis_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    SyncManagedRateLimiter,
)
from redis_rate_limiter.core.limiters import build_enhanced_payload

logger = logging.getLogger(__name__)


# noinspection PyUnnecessaryCast
class DramatiqRateLimiter(SyncManagedRateLimiter, AbstractDistributedRateLimiter):
    """A rate limiter that dispatches tasks via the Dramatiq actor framework.

    Instances should be obtained through the class methods ``configure``,
    ``create``, ``get``, and ``update`` rather than through direct construction.

    ``max_concurrency`` should match the total number of Dramatiq worker threads
    across all processes (i.e., ``--processes`` multiplied by ``--threads``).
    Dispatched tasks hold a concurrency lease while they wait in the broker
    queue. If ``max_concurrency`` exceeds the actual worker capacity, tasks
    accumulate in the broker, their leases expire, and the drain loop dispatches
    replacements, leading to unbounded queue growth.
    """

    _broker: ClassVar[Optional[dramatiq.Broker]] = None

    # ---------------------------------------------------------------------------
    # Managed backend hooks
    # ---------------------------------------------------------------------------

    @classmethod
    def configure(cls, redis_client: Redis, **backend_context: Any) -> None:
        """Configure the shared Redis client and Dramatiq broker context for the class-level API.

        Args:
            redis_client: The Redis client instance used for rate limiting state.
            **backend_context: Backend-specific keyword arguments. The ``broker``
                keyword argument is required
                (e.g., ``DramatiqRateLimiter.configure(redis, broker=broker)``).
        """
        super().configure(redis_client, **backend_context)

    @classmethod
    def _configure_backend(cls, **backend_context: Any) -> None:
        """Store the backend-specific context required by Dramatiq-backed limiter instances.

        Raises:
            RuntimeError: If the ``broker`` keyword argument is not provided.
        """
        broker = backend_context.get("broker")
        if broker is None:
            # fmt: off
            raise RuntimeError(
                "DramatiqRateLimiter.configure(redis_client, broker=broker) must be called before create() or get()."
            )
            # fmt: on
        cls._broker = broker
        dramatiq.set_broker(broker)

    @classmethod
    def _has_backend_context(cls) -> bool:
        return cls._broker is not None

    @classmethod
    def _get_instance_context(cls) -> dict[str, Any]:
        assert cls._broker is not None
        return {"broker": cls._broker}

    @classmethod
    def _reset_backend_context(cls) -> None:
        cls._broker = None

    @classmethod
    def _configure_hint(cls) -> str:
        return "DramatiqRateLimiter.configure(redis_client, broker=broker)"

    # ---------------------------------------------------------------------------
    # Instance construction
    # ---------------------------------------------------------------------------

    def __init__(
        self,
        redis_client: Redis,
        broker: dramatiq.Broker,
        *,
        _sentinel: Any = None,
        **kwargs: Any,
    ):
        """Construct a Dramatiq rate limiter instance through the managed class API.

        Args:
            redis_client: The Redis client used for state management.
            broker: The Dramatiq ``Broker`` instance used for message dispatch.
            _sentinel: An internal sentinel value supplied by the class API methods.

        All remaining parameters are inherited from ``AbstractDistributedRateLimiter``.
        """
        super().__init__(redis_client, _sentinel=_sentinel, **kwargs)
        self.broker = broker

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
            from redis_rate_limiter.backends.dramatiq.tasks.worker import (
                generic_rate_limited_worker,
            )

            generic_rate_limited_worker.send(
                limiter_id=self.id,
                func_path=func_path,
                payload=data,
                _rate_limit_task_id=task_id,
            )
            logger.debug(
                "[DramatiqRateLimiter] Message sent to generic worker: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
        else:
            from redis_rate_limiter.core.importing import import_string

            task_actor = import_string(func_path)
            task_actor.send(data, _rate_limit_task_id=task_id)
            logger.debug(
                "[DramatiqRateLimiter] Message sent to custom actor: limiter=%s, task_id=%s, func_path=%s.",
                self.id,
                task_id,
                func_path,
            )
