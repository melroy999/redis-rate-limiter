"""Fixtures for generic implementation tests.

This module provides fixtures for testing implementations that do not require
backend-specific functionality. Both sync and async test helpers are provided;
sync helpers extend ``AbstractDistributedRateLimiter``, while async helpers
extend ``AbstractAsyncDistributedRateLimiter``.

Fixture dependencies from the root ``tests/conftest.py``:
    - ``redis_client``: sync Redis client with per-test namespace isolation.
    - ``async_redis_client``: async Redis client with per-test namespace isolation.
    - ``limiter_id``: unique per-test limiter identifier.
"""

from typing import Literal
from uuid import uuid4

import pytest

from redis_rate_limiter import (
    AbstractAsyncDistributedRateLimiter,
    AbstractDistributedRateLimiter,
)

# ---------------------------------------------------------------------------
# Shared test configuration
# ---------------------------------------------------------------------------

DEFAULT_LIMITER_CONFIG: dict = dict(
    limit=5, window=60, max_concurrency=2, max_age=3600, lease_duration=30
)
"""Default rate limiter parameters used across test fixtures."""

HeartbeatFailureMode = Literal["warn", "kill"]
HEARTBEAT_OVERRIDE_CASES: list[tuple[HeartbeatFailureMode, HeartbeatFailureMode]] = [
    ("warn", "kill"),
    ("kill", "warn"),
]
"""Parametrize cases for testing heartbeat failure mode overrides.

Used by both ``test_task_lifecycle.py`` and ``test_async_task_lifecycle.py``
to verify that the limiter-level and task-level override modes interact
correctly in both directions.
"""

# ---------------------------------------------------------------------------
# Sync test helpers
# ---------------------------------------------------------------------------


class _TrackingMixin:
    """Mixin that records all dispatch and drain scheduling invocations.

    Shared by both ``TrackingRateLimiter`` and ``AsyncTrackingRateLimiter``
    to avoid duplicating the ``__init__``, ``dispatched_tasks``,
    ``scheduled_drains``, and ``_schedule_drain`` implementations.
    Subclasses must still define ``_dispatch_task`` because the sync and
    async variants differ in signature (``def`` vs ``async def``).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dispatched_tasks: list[dict] = []
        self.scheduled_drains: list[float] = []

    def _schedule_drain(self, delay: float = 0.0) -> None:
        """Record the scheduled drain delay for subsequent assertion."""
        self.scheduled_drains.append(delay)


class StubRateLimiter(AbstractDistributedRateLimiter):
    """Rate limiter whose dispatch and drain hooks perform no work.

    This allows tests to exercise all inherited core logic (consume, schedule,
    lifecycle) without any tasks being dispatched or drain cycles triggered.
    """

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        pass


class TrackingRateLimiter(_TrackingMixin, StubRateLimiter):
    """Concrete rate limiter that records all dispatch and drain scheduling
    invocations."""

    def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Record the dispatch invocation for subsequent test assertion."""
        self.dispatched_tasks.append(
            {"func_path": func_path, "payload": payload, "task_id": task_id}
        )


# ---------------------------------------------------------------------------
# Async test helpers
# ---------------------------------------------------------------------------


class AsyncStubRateLimiter(AbstractAsyncDistributedRateLimiter):
    """Async rate limiter whose dispatch and drain hooks perform no work.

    This allows tests to exercise all inherited async core logic without any
    tasks being dispatched or drain cycles triggered.
    """

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        pass

    def _schedule_drain(self, delay: float = 0.0) -> None:
        pass


class AsyncStubWithHealthCheck(AsyncStubRateLimiter):
    """Async stub that overrides ``_check_backend_health()`` to trigger
    health monitor creation in ``__init__``."""

    async def _check_backend_health(self) -> bool:
        return True


class AsyncTrackingRateLimiter(_TrackingMixin, AsyncStubRateLimiter):
    """Async concrete rate limiter that records all dispatch and drain
    scheduling invocations."""

    async def _dispatch_task(self, func_path: str, payload: dict, task_id: str) -> None:
        """Record the dispatch invocation for subsequent test assertion."""
        self.dispatched_tasks.append(
            {"func_path": func_path, "payload": payload, "task_id": task_id}
        )


# ---------------------------------------------------------------------------
# Sync fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def task_id():
    """Produce a realistic, non-hardcoded task identifier so tests do not
    accidentally depend on a specific string."""
    return f"task_{uuid4().hex[:8]}"


@pytest.fixture
def stub_limiter(redis_client, limiter_id):
    # Setup
    limiter_id = f"{limiter_id}_generic"
    test_limiter = StubRateLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()


@pytest.fixture
def tracking_limiter(redis_client, limiter_id):
    # Setup
    limiter_id = f"{limiter_id}_tracking"
    test_limiter = TrackingRateLimiter(
        redis_client=redis_client,
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
    )

    yield test_limiter

    # Teardown
    test_limiter.shutdown()


@pytest.fixture
def make_limiter_pool(redis_client, limiter_id):
    """Create N rate limiter instances sharing the same Redis state.

    This simulates N independent workers all rate-limited by the same key,
    as they would be in a multi-process deployment.
    """
    # Setup
    created_limiters: list = []

    def _factory(n, *, limiter_cls=StubRateLimiter, **kwargs):
        pool_limiter_id = f"{limiter_id}_concurrent"
        defaults = dict(DEFAULT_LIMITER_CONFIG)
        defaults.update(kwargs)
        limiters = [
            limiter_cls(
                redis_client=redis_client, limiter_id=pool_limiter_id, **defaults
            )
            for _ in range(n)
        ]
        created_limiters.extend(limiters)
        return limiters

    yield _factory

    # Teardown
    for lim in created_limiters:
        lim.shutdown()


# ---------------------------------------------------------------------------
# Async fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def async_stub_limiter(async_redis_client, limiter_id):
    # Setup
    limiter_id = f"{limiter_id}_async_generic"
    test_limiter = AsyncStubRateLimiter(
        redis_client=async_redis_client,
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
    )
    await test_limiter.start()

    yield test_limiter

    # Teardown
    await test_limiter.shutdown()


@pytest.fixture
async def async_tracking_limiter(async_redis_client, limiter_id):
    # Setup
    limiter_id = f"{limiter_id}_async_tracking"
    test_limiter = AsyncTrackingRateLimiter(
        redis_client=async_redis_client,
        limiter_id=limiter_id,
        **DEFAULT_LIMITER_CONFIG,
    )
    await test_limiter.start()

    yield test_limiter

    # Teardown
    await test_limiter.shutdown()


@pytest.fixture
async def make_async_limiter_pool(async_redis_client, limiter_id):
    """Create N async rate limiter instances sharing the same Redis state.

    This simulates N independent workers all rate-limited by the same key,
    as they would be in a multi-process deployment.
    """
    # Setup
    created_limiters: list = []

    async def _factory(n, *, limiter_cls=AsyncStubRateLimiter, **kwargs):
        pool_limiter_id = f"{limiter_id}_async_concurrent"
        defaults = dict(DEFAULT_LIMITER_CONFIG)
        defaults.update(kwargs)
        limiters = []
        for _ in range(n):
            lim = limiter_cls(
                redis_client=async_redis_client,
                limiter_id=pool_limiter_id,
                **defaults,
            )
            await lim.start()
            limiters.append(lim)
        created_limiters.extend(limiters)
        return limiters

    yield _factory

    # Teardown
    for lim in created_limiters:
        await lim.shutdown()
