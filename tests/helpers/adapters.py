"""Adapters for running unified async contract tests against sync implementations.

These adapters wrap synchronous rate limiters and distributed locks so that
async contract tests can ``await`` their methods. The underlying sync Redis
calls block the event loop briefly, but this is acceptable in a test context
where no other async work is running concurrently.
"""


class SyncToAsyncLimiterAdapter:
    """Wraps a sync ``AbstractDistributedRateLimiter`` as an async-compatible limiter.

    Async contract tests can ``await`` the adapter's methods, which delegate
    to the underlying synchronous implementation.
    """

    def __init__(self, inner):
        self._inner = inner

    async def schedule_task(self, *args, **kwargs):
        return self._inner.schedule_task(*args, **kwargs)

    async def consume(self, *args, **kwargs):
        return self._inner.consume(*args, **kwargs)

    async def get_buffer_count(self):
        return self._inner.get_buffer_count()

    async def get_status(self, *args, **kwargs):
        return self._inner.get_status(*args, **kwargs)

    def get_inflight_key(self, task_id):
        return self._inner.get_inflight_key(task_id)

    def execution_lock(self, **kwargs):
        return SyncToAsyncLockAdapter(self._inner.execution_lock(**kwargs))

    def __getattr__(self, name):
        return getattr(self._inner, name)


class SyncToAsyncLockAdapter:
    """Wraps a sync ``DistributedLock`` as an async context manager.

    The sync ``__enter__``/``__exit__`` calls are delegated from
    ``__aenter__``/``__aexit__`` so that ``async with`` works transparently.
    """

    def __init__(self, inner):
        self._inner = inner

    async def __aenter__(self):
        return self._inner.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self._inner.__exit__(exc_type, exc_val, exc_tb)

    @property
    def token(self):
        return self._inner.token
