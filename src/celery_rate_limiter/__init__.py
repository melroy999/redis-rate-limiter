"""A distributed rate limiter with pluggable task execution backends."""

from celery_rate_limiter.core import (
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
    "CeleryRateLimiter",
    "DistributedLock",
    "PrometheusMetricsExporter",
    "RateLimitMiddleware",
    "TaskLifecycle",
    "ThreadPoolRateLimiter",
    "import_string",
    "rate_limited",
    "resolve_import_path",
]

# The Celery backend is only available when the celery package is installed.
try:
    from celery_rate_limiter.backends.celery import CeleryRateLimiter
except ImportError:
    pass

# The threading backend relies solely on the standard library and does not require
# any external dependencies. As such, it is included by default.
# The ASGI backend provides framework-agnostic rate limiting middleware.
from celery_rate_limiter.backends.asgi import ASGIRateLimiter, RateLimitMiddleware

# The asyncio backend relies solely on the standard library and redis.asyncio.
from celery_rate_limiter.backends.asyncio import AsyncIOTaskLimiter
from celery_rate_limiter.backends.threading import ThreadPoolRateLimiter

# The Prometheus integration is only available when the prometheus_client package is installed.
try:
    from celery_rate_limiter.integrations.prometheus import PrometheusMetricsExporter
except ImportError:
    pass
