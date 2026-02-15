"""A distributed rate limiter with pluggable task execution backends."""

from celery_rate_limiter.core import (
    AbstractDistributedRateLimiter,
    AbstractRedisManagedRateLimiter,
    DistributedLock,
    TaskLifecycle,
    import_string,
    rate_limited,
)

__all__ = [
    "AbstractDistributedRateLimiter",
    "AbstractRedisManagedRateLimiter",
    "CeleryRateLimiter",
    "DistributedLock",
    "PrometheusMetricsExporter",
    "TaskLifecycle",
    "ThreadPoolRateLimiter",
    "import_string",
    "rate_limited",
]

# The Celery backend is only available when the celery package is installed.
try:
    from celery_rate_limiter.backends.celery import CeleryRateLimiter
except ImportError:
    pass

# The threading backend relies solely on the standard library and does not require
# any external dependencies. As such, it is included by default.
from celery_rate_limiter.backends.threading import ThreadPoolRateLimiter

# The Prometheus integration is only available when the prometheus_client package is installed.
try:
    from celery_rate_limiter.integrations.prometheus import PrometheusMetricsExporter
except ImportError:
    pass
