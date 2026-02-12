![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/melroy999/f3caa8f0af98bf11563b5b2031c1ef3e/raw/celery-rate-limiter-coverage.json)

# celery-rate-limiter

A distributed rate limiter for Python with pluggable task backends. Rate limiting state lives in Redis--multiple processes and machines sharing the same limiter ID are collectively rate-limited, regardless of how tasks are dispatched.

The core algorithm is a sliding window counter implemented as atomic Lua scripts, which gives smooth rate transitions without the burstiness of fixed windows or the memory cost of a pure sliding log.

## Features

- **Sliding window counter**: smooth rate limiting without sudden token resets at window boundaries.
- **Concurrency control**: lease-based concurrency slots with automatic expiry, so crashed workers don't permanently consume capacity.
- **Task deduplication**: identical tasks (same function + payload) are deduplicated via atomic Redis markers.
- **Priority queue**: tasks are buffered in a Redis sorted set, consumed in priority order.
- **Dead letter queue**: tasks that exceed their maximum age are moved to a DLQ instead of being silently dropped.
- **Dynamic configuration**: rate limits, concurrency caps, and window sizes can be changed in Redis at runtime. All existing limiter instances across workers and machines pick up the new configuration on their next drain cycle.
- **Smart jitter**: adaptive retry delays that scale with queue depth and concurrency pressure to prevent thundering herd at window resets (see [docs/smart-jitter.md](docs/smart-jitter.md)).
- **Metrics callbacks**: optional hook for observability, invoked after every consume and schedule operation.

## Installation

```bash
# Core (Redis only, no task backend).
pip install celery-rate-limiter

# With the Celery backend.
pip install celery-rate-limiter[celery]
```

Requires Python 3.12+ and a single Redis instance (not Redis Cluster--see the class docstring for details).

## Quick Start (Celery)

```python
import redis
from celery import Celery
from celery_rate_limiter import CeleryRateLimiter

# One-time setup.
redis_client = redis.Redis(host="localhost", port=6379)
celery_app = Celery("myapp", broker="redis://localhost:6379/0")
CeleryRateLimiter.configure(redis_client, celery_app=celery_app)

# Create a limiter (idempotent with override=True).
limiter = CeleryRateLimiter.create(
    limiter_id="api_calls",
    limit=100,              # 100 tasks per window
    window=60,              # 60 second window
    max_concurrency=10,     # at most 10 running at once
    override=True,
)

# Schedule a rate-limited task.
success, task_id = limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Check limiter status at any time.
status = limiter.get_status()
```

The `examples/` directory contains a full working demo with a Celery worker, task simulator, and a live status inspector.

## How It Works

1. `schedule_task()` adds a task to a Redis priority queue (with deduplication).
2. A drain loop acquires a distributed lock and calls `consume()`.
3. `consume()` runs a Lua script that atomically checks the sliding window counter, verifies concurrency capacity, and pops the next task from the buffer.
4. The task is dispatched to the configured backend (Celery, thread pool, etc.).
5. The worker holds a concurrency lease that is renewed via a heartbeat thread. If the worker crashes, the lease expires and the slot is reclaimed automatically.
6. On completion (or failure), the concurrency slot is released and the next drain is triggered.

## Backend Roadmap

All backends extend `AbstractRedisManagedRateLimiter` to share the same distributed rate limiting state and dynamic configuration support. They differ only in how tasks are dispatched for execution.

| Backend          | Dispatch mechanism                       | Status  |
|------------------|------------------------------------------|---------|
| Celery           | Celery broker (`send_task`)              | Done    |
| Threading        | `concurrent.futures.ThreadPoolExecutor`  | Planned |
| AsyncIO          | `asyncio` event loop / task group        | Planned |
| Multiprocessing  | `concurrent.futures.ProcessPoolExecutor` | Planned |
| RQ (Redis Queue) | RQ job queue                             | Planned |
| Dramatiq         | Dramatiq broker                          | Planned |
| ASGI Middleware  | Starlette/FastAPI request handling       | Planned |

### Class hierarchy

```
AbstractDistributedRateLimiter        -- distributed rate limiting via Redis
└── AbstractRedisManagedRateLimiter   -- adds singleton registry + dynamic config
    ├── CeleryRateLimiter             -- dispatches via Celery
    ├── ThreadPoolRateLimiter         -- dispatches to thread pool
    ├── AsyncIORateLimiter            -- dispatches to event loop
    ├── ProcessPoolRateLimiter        -- dispatches to process pool
    ├── RQRateLimiter                 -- dispatches via RQ
    ├── DramatiqRateLimiter           -- dispatches via Dramatiq
    └── ASGIRateLimiterMiddleware     -- rate limits HTTP requests
```

### Use case examples

**Threading / AsyncIO / Multiprocessing**: Rate-limited outbound API calls from a single application. For example, a scraper or data pipeline that must respect a third-party API's rate limit (e.g. 100 req/min to Stripe) while running many tasks concurrently. Multiple instances of the application share the same Redis-backed limit, so scaling horizontally doesn't violate the quota.

**Celery / RQ / Dramatiq**: Distributed background job processing. For example, a SaaS platform that sends webhook deliveries, email campaigns, or report generation jobs across a fleet of workers, all collectively capped to protect downstream services.

**ASGI Middleware**: Rate limiting both sides of the HTTP boundary. On the *inbound* side, it protects your own API from being overwhelmed (e.g. 1000 req/min per API key across all FastAPI replicas). On the *outbound* side, the same middleware can gate proxy/forwarding routes that call upstream services, ensuring your fleet collectively stays within the upstream provider's limits. Because the state lives in Redis, the limit is enforced globally, not per-replica.

## Testing

The test suite is organized into contract, implementation, algorithm, property-based (Hypothesis), and integration tests. All tests require a running Redis instance. See [tests/README.md](tests/README.md) for the full breakdown.

```bash
# Run tests (excludes slow tests by default).
poetry run pytest

# Include slow tests (real time.sleep, multiple configurations).
poetry run pytest --override-ini='addopts=' tests/

# Run inside Docker (recommended for integration tests).
docker compose --profile test up
```

See [DOCKER.md](DOCKER.md) for Docker Compose setup and common scenarios.

## Development

```bash
# Formatting and linting.
poetry run ruff check --select I --fix .
poetry run ruff format .
poetry run ruff check .

# Type checking.
poetry run mypy src

# Coverage report.
poetry run pytest tests/ --cov=celery_rate_limiter --cov-report=html
```
