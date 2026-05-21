"""A distributed rate limiter with pluggable task execution backends."""

from redis_rate_limiter.core import (
    AbstractAsyncDistributedRateLimiter,
    AbstractDistributedRateLimiter,
    DistributedLock,
    TaskLifecycle,
    import_string,
    rate_limited,
    resolve_import_path,
)

__all__ = [
    "AbstractAsyncDistributedRateLimiter",
    "AbstractDistributedRateLimiter",
    "ASGIRateLimiter",
    "AsyncIOTaskLimiter",
    "DistributedLock",
    "ProcessPoolRateLimiter",
    "RateLimitMiddleware",
    "TaskLifecycle",
    "ThreadPoolRateLimiter",
    "import_string",
    "rate_limited",
    "resolve_import_path",
]

try:
    from redis_rate_limiter.backends.celery import CeleryRateLimiter  # noqa: F401

    __all__.append("CeleryRateLimiter")
except ImportError:
    pass

# ValueError is caught because rq calls get_context("fork") at import time,
# which raises ValueError on Windows (no fork support).
try:
    from redis_rate_limiter.backends.rq import RQRateLimiter  # noqa: F401

    __all__.append("RQRateLimiter")
except (ImportError, ValueError):
    pass

try:
    from redis_rate_limiter.backends.dramatiq import DramatiqRateLimiter  # noqa: F401

    __all__.append("DramatiqRateLimiter")
except ImportError:
    pass

try:
    from redis_rate_limiter.backends.huey import HueyRateLimiter  # noqa: F401

    __all__.append("HueyRateLimiter")
except ImportError:
    pass

from redis_rate_limiter.backends.asgi import ASGIRateLimiter, RateLimitMiddleware
from redis_rate_limiter.backends.asyncio import AsyncIOTaskLimiter
from redis_rate_limiter.backends.processpool import ProcessPoolRateLimiter
from redis_rate_limiter.backends.threading import ThreadPoolRateLimiter

try:
    from redis_rate_limiter.integrations.prometheus import (
        PrometheusMetricsExporter,  # noqa: F401
    )

    __all__.append("PrometheusMetricsExporter")
except ImportError:
    pass
