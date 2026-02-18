"""AsyncIO backend for distributed, rate-limited task execution."""

from celery_rate_limiter.backends.asyncio.limiter import AsyncIOTaskLimiter

__all__ = ["AsyncIOTaskLimiter"]
