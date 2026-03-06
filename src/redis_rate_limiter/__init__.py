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

# The Celery backend is only available when the celery package is installed.
try:
    from redis_rate_limiter.backends.celery import CeleryRateLimiter

    __all__.append("CeleryRateLimiter")
except ImportError:
    pass

# The RQ backend is only available when the rq package is installed.
try:
    from redis_rate_limiter.backends.rq import RQRateLimiter

    __all__.append("RQRateLimiter")
except ImportError:
    pass

# The threading backend relies solely on the standard library and does not require
# any external dependencies. As such, it is included by default.
# The ASGI backend provides framework-agnostic rate limiting middleware.
from redis_rate_limiter.backends.asgi import ASGIRateLimiter, RateLimitMiddleware

# The asyncio backend relies solely on the standard library and redis.asyncio.
from redis_rate_limiter.backends.asyncio import AsyncIOTaskLimiter

# The process pool and thread pool backends rely solely on the standard library.
from redis_rate_limiter.backends.processpool import ProcessPoolRateLimiter
from redis_rate_limiter.backends.threading import ThreadPoolRateLimiter

# The Prometheus integration is only available when the prometheus_client package is installed.
try:
    from redis_rate_limiter.integrations.prometheus import PrometheusMetricsExporter

    __all__.append("PrometheusMetricsExporter")
except ImportError:
    pass
