[![CI](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/melroy999/f3caa8f0af98bf11563b5b2031c1ef3e/raw/redis-rate-limiter-ci.json)](https://github.com/melroy999/redis-rate-limiter/actions/workflows/ci.yml) [![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/melroy999/f3caa8f0af98bf11563b5b2031c1ef3e/raw/redis-rate-limiter-coverage.json)](https://github.com/melroy999/redis-rate-limiter/actions/workflows/ci.yml) [![Mutation Score](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/melroy999/f3caa8f0af98bf11563b5b2031c1ef3e/raw/redis-rate-limiter-mutation-score.json)](https://github.com/melroy999/redis-rate-limiter/actions/workflows/mutation.yml) [![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/) [![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![mypy](https://img.shields.io/badge/type%20checking-mypy%20strict-blue)](http://mypy-lang.org/)

# redis-rate-limiter

A distributed rate limiter for Python with pluggable task backends. Rate limiting state is stored in Redis, which means that multiple processes and machines sharing the same limiter ID are collectively rate-limited, regardless of how tasks are dispatched.

The core algorithm is a sliding window counter implemented as atomic Lua scripts, which provides smooth rate transitions without the burstiness of fixed windows or the memory cost of a pure sliding log.

## Features

- **Sliding window counter**: smooth rate limiting without sudden token resets at window boundaries.
- **Sync and async support**: both synchronous (threading, Celery) and asynchronous (asyncio, ASGI) backends, sharing the same Redis-backed rate limiting state.
- **Concurrency control**: lease-based concurrency slots with automatic expiry, such that crashed workers do not permanently consume capacity.
- **Task deduplication**: identical tasks, i.e., tasks with the same function and payload, are deduplicated via atomic Redis markers.
- **Priority queue**: tasks are buffered in a Redis sorted set and consumed in priority order.
- **Dead letter queue**: tasks that exceed their maximum age are moved to a DLQ instead of being silently dropped.
- **Dynamic configuration**: rate limits, concurrency caps and window sizes can be changed in Redis at runtime. All existing limiter instances across workers and machines pick up the new configuration on their next drain cycle.
- **Smart jitter**: adaptive retry delays that scale with queue depth and concurrency pressure to prevent the thundering herd problem at window resets (see [docs/smart-jitter.md](docs/smart-jitter.md)).
- **ASGI middleware**: request-level rate limiting for FastAPI with per-client identity keys, standard rate limit headers and configurable bypass rules.
- **Metrics callbacks**: an optional hook for observability, invoked after every consume and schedule operation.

## Installation

```bash
# Core (Redis only, no task backend).
pip install redis-rate-limiter

# With the Celery backend.
pip install redis-rate-limiter[celery]

# With the RQ backend.
pip install redis-rate-limiter[rq]

# With the Dramatiq backend.
pip install redis-rate-limiter[dramatiq]

# With the Huey backend.
pip install redis-rate-limiter[huey]

# With the ASGI middleware backend (FastAPI).
pip install redis-rate-limiter[asgi]

# All optional dependencies.
pip install redis-rate-limiter[celery,rq,dramatiq,huey,asgi,prometheus]
```

The project requires Python 3.12+ and a single Redis instance (not Redis Cluster, see the class docstring for details).

## Quick Start (Celery)

```python
import redis
from celery import Celery
from redis_rate_limiter import CeleryRateLimiter

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

# Stop background threads when done.
limiter.shutdown()
```

### Quick Start (Thread Pool)

```python
import redis
from concurrent.futures import ThreadPoolExecutor
from redis_rate_limiter import ThreadPoolRateLimiter

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
executor = ThreadPoolExecutor(max_workers=4)
ThreadPoolRateLimiter.configure(redis_client, executor=executor)

limiter = ThreadPoolRateLimiter.create(
    limiter_id="api_calls",
    limit=100,
    window=60,
    max_concurrency=10,
    override=True,
)

success, task_id = limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Stop background threads when done.
limiter.shutdown()
```

### Quick Start (Process Pool)

```python
import redis
from concurrent.futures import ProcessPoolExecutor
from redis_rate_limiter import ProcessPoolRateLimiter

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
executor = ProcessPoolExecutor(max_workers=4)
ProcessPoolRateLimiter.configure(redis_client, executor=executor)

limiter = ProcessPoolRateLimiter.create(
    limiter_id="api_calls",
    limit=100,
    window=60,
    max_concurrency=10,
    override=True,
)

success, task_id = limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Stop background threads when done.
limiter.shutdown()
executor.shutdown(wait=True)
```

Task functions and their payloads must be picklable (i.e., module-level functions, not closures or lambdas) because they are serialized and sent to child processes. The task lifecycle (heartbeat, lease management) is managed in the parent process.

### Quick Start (RQ)

```python
import redis
from rq import Queue
from redis_rate_limiter import RQRateLimiter

redis_client = redis.Redis(host="localhost", port=6379)
queue = Queue(connection=redis_client)
RQRateLimiter.configure(redis_client, queue=queue)

limiter = RQRateLimiter.create(
    limiter_id="api_calls",
    limit=100,
    window=60,
    max_concurrency=10,
    override=True,
)

success, task_id = limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Stop background threads when done.
limiter.shutdown()
```

The RQ worker process must have `RQRateLimiter` configured before processing jobs, so that the `@rate_limited` decorator can resolve the limiter via `RQRateLimiter.get()`. See [examples/rq/demo.py](examples/rq/demo.py) for a complete working example.

### Quick Start (Dramatiq)

```python
import redis
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from redis_rate_limiter import DramatiqRateLimiter

redis_client = redis.Redis(host="localhost", port=6379)
broker = RedisBroker(host="localhost", port=6379)
dramatiq.set_broker(broker)
DramatiqRateLimiter.configure(redis_client, broker=broker)

limiter = DramatiqRateLimiter.create(
    limiter_id="api_calls",
    limit=100,
    window=60,
    max_concurrency=10,
    override=True,
)

success, task_id = limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Stop background threads when done.
limiter.shutdown()
```

The Dramatiq worker process must have `DramatiqRateLimiter` configured and the broker set before processing messages, so that the `@rate_limited` decorator can resolve the limiter via `DramatiqRateLimiter.get()`. See [examples/dramatiq/demo.py](examples/dramatiq/demo.py) for a complete working example.

### Quick Start (Huey)

```python
import redis
from huey import RedisHuey
from redis_rate_limiter import HueyRateLimiter

redis_client = redis.Redis(host="localhost", port=6379)
huey = RedisHuey("myapp", host="localhost", port=6379)
HueyRateLimiter.configure(redis_client, huey=huey)

limiter = HueyRateLimiter.create(
    limiter_id="api_calls",
    limit=100,
    window=60,
    max_concurrency=10,
    override=True,
)

success, task_id = limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Stop background threads when done.
limiter.shutdown()
```

The Huey consumer process must have `HueyRateLimiter` configured before processing tasks, so that the `@rate_limited` decorator can resolve the limiter via `HueyRateLimiter.get()`. See [examples/huey/demo.py](examples/huey/demo.py) for a complete working example.

### Quick Start (AsyncIO)

```python
import redis.asyncio
from redis_rate_limiter import AsyncIOTaskLimiter

redis_client = redis.asyncio.Redis(host="localhost", port=6379, decode_responses=True)
AsyncIOTaskLimiter.configure(redis_client, max_tasks=10)

limiter = await AsyncIOTaskLimiter.create(
    limiter_id="api_calls",
    limit=100,
    window=60,
    max_concurrency=10,
    override=True,
)

success, task_id = await limiter.schedule_task(
    "myapp.services.call_external_api",
    {"user_id": 42},
)

# Stop background tasks when done.
await limiter.shutdown()
```

### Quick Start (ASGI Middleware)

```python
import redis.asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis_rate_limiter.backends.asgi import ASGIRateLimiter, by_client_ip

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_client = redis.asyncio.Redis(host="localhost", port=6379, decode_responses=True)
    ASGIRateLimiter.configure(redis_client)
    app.state.limiter = await ASGIRateLimiter.create(
        limiter_id="api_gateway",
        limit=1000,
        window=60,
        override=True,
    )
    yield
    await redis_client.aclose()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    key = by_client_ip(request.scope)
    if key is None:
        return await call_next(request)
    result = await request.app.state.limiter.acquire(key)
    if not result["allowed"]:
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded."})
    return await call_next(request)
```

The `by_client_ip` key function extracts the client IP from the ASGI scope. For header-based identity (e.g., API key), use `by_header("x-api-key")` instead. For the direct ASGI wrapper approach using `RateLimitMiddleware`, see [examples/asgi/demo.py](examples/asgi/demo.py).

The `examples/` directory contains full working demos for each backend with task simulators and live status dashboards.

## How It Works

1. `schedule_task()` adds a task to a Redis priority queue, with deduplication.
2. A drain loop acquires a distributed lock and calls `consume()`.
3. `consume()` runs a Lua script that atomically checks the sliding window counter, verifies the concurrency capacity and pops the next task from the buffer.
4. The task is dispatched to the configured backend (Celery, thread pool, asyncio, etc.).
5. The worker holds a concurrency lease that is renewed via a background heartbeat (thread or asyncio task, depending on the backend). If the worker crashes, the lease expires and the slot is reclaimed automatically.
6. On completion or failure, the concurrency slot is released and the next drain is triggered.

The ASGI middleware follows a simpler path: `acquire()` atomically checks the sliding window counter for a given client identity key and returns an allow/deny decision with standard rate limit headers. There is no task buffer, concurrency tracking or drain loop.

### Sizing `max_concurrency` for External Broker Backends

For the in-process backends (Threading, Multiprocessing, AsyncIO), the rate limiter has direct visibility into executor capacity and will not dispatch tasks that would only be queued locally. For external broker backends (Celery, RQ, Dramatiq, Huey), this visibility does not exist: dispatched tasks sit in the broker queue until a worker picks them up.

Each dispatched task holds a concurrency lease (renewed via heartbeat once the worker starts processing). If `max_concurrency` exceeds the actual worker fleet capacity, tasks accumulate in the broker queue faster than workers can process them. Their leases expire, freeing concurrency slots, and the drain loop dispatches replacements into those freed slots, which also accumulate. This cycle leads to unbounded queue growth.

To avoid this, set `max_concurrency` to match the total number of task slots across the worker fleet:

| Backend  | Worker capacity formula |
|----------|------------------------|
| Celery   | Sum of `--concurrency` across all workers |
| RQ       | Total number of workers listening on the queue |
| Dramatiq | `--processes` multiplied by `--threads` |
| Huey     | `--workers` flag passed to the Huey consumer |

## Backend Roadmap

All task-oriented backends compose `SyncManagedRateLimiter` (or `AsyncManagedRateLimiter`) with `AbstractDistributedRateLimiter` (or its async counterpart) to share the same distributed rate limiting state and dynamic configuration support. They differ only in how tasks are dispatched for execution.

| Backend          | Dispatch mechanism                       | Status  |
|------------------|------------------------------------------|---------|
| Celery           | Celery broker (`send_task`)              | Done    |
| Threading        | `concurrent.futures.ThreadPoolExecutor`  | Done    |
| AsyncIO          | `asyncio` event loop / task group        | Done    |
| ASGI Middleware  | FastAPI request handling                 | Done    |
| Multiprocessing  | `concurrent.futures.ProcessPoolExecutor` | Done    |
| RQ (Redis Queue) | RQ job queue (`queue.enqueue`)           | Done    |
| Dramatiq         | Dramatiq broker (`actor.send`)           | Done    |
| Huey             | Huey task queue (`huey_task()`)           | Done    |

### Class Hierarchy

```
SyncManagedRateLimiter + AbstractDistributedRateLimiter     -- sync managed + distributed
    ├── CeleryRateLimiter                                   -- dispatches via Celery
    ├── DramatiqRateLimiter                                 -- dispatches via Dramatiq
    ├── HueyRateLimiter                                     -- dispatches via Huey
    ├── ProcessPoolRateLimiter                              -- dispatches to process pool
    ├── RQRateLimiter                                       -- dispatches via RQ
    └── ThreadPoolRateLimiter                               -- dispatches to thread pool

AsyncManagedRateLimiter + AbstractAsyncDistributedRateLimiter -- async managed + distributed
    └── AsyncIOTaskLimiter                                  -- dispatches to event loop

AsyncManagedRateLimiter + AbstractAsyncRateLimiter          -- async managed (no task machinery)
    └── ASGIRateLimiter                                     -- rate limits HTTP requests
```

### Use Case Examples

**Threading / AsyncIO / Multiprocessing**: rate-limited outbound API calls from a single application. For example, a scraper or data pipeline that must respect a third-party API's rate limit (e.g., 100 req/min to Stripe) while running many tasks concurrently. Multiple instances of the application share the same Redis-backed limit, such that scaling horizontally does not violate the quota.

**Celery / RQ / Dramatiq**: distributed background job processing. For example, a SaaS platform that sends webhook deliveries, email campaigns or report generation jobs across a fleet of workers, all collectively capped to protect downstream services.

**ASGI Middleware**: rate limiting both sides of the HTTP boundary. On the *inbound* side, it protects the API from being overwhelmed (e.g., 1000 req/min per API key across all FastAPI replicas). On the *outbound* side, the same middleware can gate proxy/forwarding routes that call upstream services, ensuring the fleet collectively stays within the upstream provider's limits. Because the state is stored in Redis, the limit is enforced globally, not per-replica.

## Testing

The test suite is organized into contract, implementation, algorithm, property-based (Hypothesis) and integration tests. All tests require a running Redis instance; running them through Docker is recommended because Windows has unreliable sub-second `time.sleep()` resolution, which causes timing-sensitive integration tests to flake. See [tests/README.md](tests/README.md) for the full breakdown.

```bash
# Fast tests (excludes slow integration tests).
docker compose --profile test up --build --abort-on-container-exit --exit-code-from test

# All tests including slow integration tests.
docker compose --profile test-all up --build --abort-on-container-exit --exit-code-from test-all

# Mutation testing (manual, on-demand).
docker compose --profile mutate up --build --abort-on-container-exit --exit-code-from mutate
```

With a local Redis instance running, pytest can be invoked directly:

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
poetry run pytest tests/ --cov=redis_rate_limiter --cov-report=html
```
