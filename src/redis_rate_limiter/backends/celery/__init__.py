"""Celery backend for distributed, rate-limited task execution."""

from redis_rate_limiter.backends.celery.limiter import CeleryRateLimiter

__all__ = ["CeleryRateLimiter"]
