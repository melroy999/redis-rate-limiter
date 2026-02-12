"""Threading backend for distributed rate-limited task execution."""

from celery_rate_limiter.backends.threading.limiter import ThreadPoolRateLimiter

__all__ = ["ThreadPoolRateLimiter"]
