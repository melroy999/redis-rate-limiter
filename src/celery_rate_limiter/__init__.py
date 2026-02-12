"""Distributed rate limiter with pluggable task backends."""

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
    "TaskLifecycle",
    "ThreadPoolRateLimiter",
    "import_string",
    "rate_limited",
]

# Celery backend is only available when celery is installed.
try:
    from celery_rate_limiter.backends.celery import CeleryRateLimiter
except ImportError:
    pass

# Threading backend uses only the stdlib, which doesn't require external libraries.
# Hence, include it by default.
from celery_rate_limiter.backends.threading import ThreadPoolRateLimiter
