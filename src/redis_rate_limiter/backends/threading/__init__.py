"""Thread pool backend for distributed, rate-limited task execution."""

from redis_rate_limiter.backends.threading.limiter import ThreadPoolRateLimiter

__all__ = ["ThreadPoolRateLimiter"]
