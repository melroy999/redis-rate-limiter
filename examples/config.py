"""Central configuration for all example demonstrations.

These values may be adjusted to experiment with different rate limiting
behaviours. The Redis connection settings can also be overridden via
environment variables.
"""

import os

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

LIMIT = 25  # maximum number of tokens per window
WINDOW = 1.0  # duration of the sliding window in seconds
MAX_CONCURRENCY = 3  # maximum number of tasks executing concurrently

# ---------------------------------------------------------------------------
# Mock task
# ---------------------------------------------------------------------------

TASK_SLEEP_MIN = 0.05  # minimum simulated API latency, in seconds
TASK_SLEEP_MAX = 0.15  # maximum simulated API latency, in seconds

# ---------------------------------------------------------------------------
# Demo sequence
# ---------------------------------------------------------------------------

DEDUP_COUNT = 10  # number of identical tasks scheduled in the deduplication test
BURST_COUNT = 300  # number of unique tasks enqueued in the burst test
ERROR_COUNT = (
    3  # number of tasks that raise an exception for the error-recovery demonstration
)
PRIORITY_SEED = 42  # seed value for reproducible random task priorities

# ---------------------------------------------------------------------------
# ThreadPool backend
# ---------------------------------------------------------------------------

THREADPOOL_MAX_WORKERS = 4  # number of workers in the thread pool

# ---------------------------------------------------------------------------
# Celery backend
# ---------------------------------------------------------------------------

CELERY_WORKER_CONCURRENCY = 4  # number of concurrent worker processes
CELERY_WORKER_PREFETCH_MULTIPLIER = 1  # number of tasks prefetched per worker at a time

# ---------------------------------------------------------------------------
# AsyncIO backend
# ---------------------------------------------------------------------------

ASYNCIO_MAX_TASKS = 4  # maximum number of concurrent asyncio tasks

# ---------------------------------------------------------------------------
# ASGI middleware
# ---------------------------------------------------------------------------

ASGI_LIMIT = 10  # maximum requests per window for the ASGI demo
ASGI_WINDOW = 60.0  # duration of the sliding window in seconds
