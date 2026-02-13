"""Central configuration for all example demos.

Tweak these values to experiment with different rate limiting behaviours.
Redis connection settings can also be overridden via environment variables.
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

LIMIT = 25  # tokens per window
WINDOW = 1.0  # window duration in seconds
MAX_CONCURRENCY = 3  # max tasks executing at once

# ---------------------------------------------------------------------------
# Mock task
# ---------------------------------------------------------------------------

TASK_SLEEP_MIN = 0.05  # minimum simulated API latency (seconds)
TASK_SLEEP_MAX = 0.15  # maximum simulated API latency (seconds)

# ---------------------------------------------------------------------------
# Demo sequence
# ---------------------------------------------------------------------------

DEDUP_COUNT = 10  # identical tasks to schedule in the deduplication test
BURST_COUNT = 300  # unique tasks to queue in the burst test
ERROR_COUNT = 3  # tasks that raise an exception (error-recovery demo)
PRIORITY_SEED = 42  # seed for reproducible random task priorities

# ---------------------------------------------------------------------------
# ThreadPool backend
# ---------------------------------------------------------------------------

THREADPOOL_MAX_WORKERS = 4  # thread pool size

# ---------------------------------------------------------------------------
# Celery backend
# ---------------------------------------------------------------------------

CELERY_WORKER_PREFETCH_MULTIPLIER = 1  # tasks fetched per worker at a time
