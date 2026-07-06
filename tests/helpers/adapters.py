"""Adapters for running unified async tests against sync implementations.

These adapters wrap synchronous rate limiters and task lifecycle managers
so that async tests can ``await`` their methods.
"""


class SyncToAsyncLimiterAdapter:
    """Wraps a sync rate limiter so async tests can ``await`` its methods."""

    def __init__(self, inner):
        super().__setattr__("_inner", inner)

    def __setattr__(self, name, value):
        setattr(self._inner, name, value)

    async def schedule_task(self, *args, **kwargs):
        return self._inner.schedule_task(*args, **kwargs)

    async def consume(self, *args, **kwargs):
        return self._inner.consume(*args, **kwargs)

    async def drain(self):
        self._inner.drain()

    async def get_buffer_count(self):
        return self._inner.get_buffer_count()

    async def get_status(self, *args, **kwargs):
        return self._inner.get_status(*args, **kwargs)

    async def trigger_consume(self):
        self._inner.trigger_consume()

    async def shutdown(self):
        self._inner.shutdown()

    async def extend_lease(self, task_id, duration):
        self._inner.extend_lease(task_id, duration)

    def get_inflight_key(self, task_id):
        return self._inner.get_inflight_key(task_id)

    async def acquire(self, timeout, priority=100):
        return SyncToAsyncLifecycleAdapter(self._inner.acquire(timeout, priority))

    def task_lifecycle(self, task_id, **kwargs):
        return SyncToAsyncLifecycleAdapter(
            self._inner.task_lifecycle(task_id, **kwargs)
        )

    def __getattr__(self, name):
        return getattr(self._inner, name)


class SyncToAsyncLifecycleAdapter:
    """Wraps a sync ``TaskLifecycle`` so async tests can use ``async with``."""

    def __init__(self, inner):
        self._inner = inner

    async def __aenter__(self):
        return self._inner.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self._inner.__exit__(exc_type, exc_val, exc_tb)

    def __getattr__(self, name):
        return getattr(self._inner, name)
