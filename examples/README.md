# Examples

Self-contained demos that showcase the rate limiter with different backends. The task-oriented demos (ThreadPool, Celery, AsyncIO) run the same sequence: a **deduplication test**, an **error-recovery test** (tasks that raise exceptions to demonstrate that concurrency slots are released and processing continues) and a **burst test**, all displayed via a live terminal dashboard. The ASGI demo is a standalone FastAPI application that demonstrates per-client-IP request rate limiting via middleware.

## Prerequisites

A running Redis instance is required. One can be started with Docker:

```bash
docker-compose up redis          # exposes Redis on port 6380
```

Alternatively, a local Redis instance on the default port (6379) can be used.

## Running the Demos

### ThreadPool Demo

Runs everything in a single Python process using a `ThreadPoolExecutor`. This is the most straightforward way to observe the rate limiter in action.

```bash
# Local Redis (port 6379).
poetry run python -m examples.threadpool.demo

# Docker Redis (port 6380).
REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.threadpool.demo
```

### Celery Demo

Spawns a Celery worker subprocess automatically and coordinates tasks over Redis. This demonstrates distributed rate limiting across processes.

```bash
# Local Redis (port 6379).
poetry run python -m examples.celery.demo

# Docker Redis (port 6380).
REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.celery.demo
```

Note that the worker takes approximately 3 seconds to connect before the demo begins.

### AsyncIO Demo

Runs everything in a single Python process and a single event loop using `asyncio.create_task()`. This demonstrates that the full distributed rate limiting machinery (drain loop, Pub/Sub subscriber, task lifecycle heartbeat) can operate within a single-threaded event loop.

```bash
# Local Redis (port 6379).
poetry run python -m examples.asyncio.demo

# Docker Redis (port 6380).
REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.asyncio.demo
```

### ASGI Middleware Demo

A self-contained FastAPI application that applies per-client-IP rate limiting via middleware. The demo starts a uvicorn server, fires HTTP requests automatically, and prints formatted results showing rate limit headers (`X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`), 429 responses with `Retry-After`, and health endpoint bypass. Requires `fastapi` and `uvicorn` (available as an optional dependency group):

```bash
poetry install --extras asgi

# Local Redis (port 6379).
poetry run python -m examples.asgi.demo

# Docker Redis (port 6380).
REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.asgi.demo
```

The FastAPI app is also importable for standalone use with uvicorn:

```bash
poetry run uvicorn examples.asgi.demo:app --reload
```

## Environment Variables

| Variable     | Default     | Description    |
|--------------|-------------|----------------|
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379`      | Redis port     |

## Configuration

All tuneable parameters are defined in [config.py](config.py):

| Parameter                           | Description                                |
|-------------------------------------|--------------------------------------------|
| `REDIS_HOST`                        | Redis hostname (env override supported)    |
| `REDIS_PORT`                        | Redis port (env override supported)        |
| `LIMIT`                             | Tokens allowed per window                  |
| `WINDOW`                            | Window duration in seconds                 |
| `MAX_CONCURRENCY`                   | Maximum number of tasks executing concurrently |
| `TASK_SLEEP_MIN`                    | Minimum simulated API latency (seconds)    |
| `TASK_SLEEP_MAX`                    | Maximum simulated API latency (seconds)    |
| `DEDUP_COUNT`                       | Number of identical tasks in the deduplication test |
| `ERROR_COUNT`                       | Number of tasks that raise an exception (error-recovery test) |
| `BURST_COUNT`                       | Number of unique tasks queued in the burst test |
| `PRIORITY_SEED`                     | Seed for reproducible random task priorities |
| `THREADPOOL_MAX_WORKERS`            | Thread pool size                           |
| `CELERY_WORKER_CONCURRENCY`         | Number of Celery worker processes          |
| `CELERY_WORKER_PREFETCH_MULTIPLIER` | Tasks fetched per Celery worker at a time  |
| `ASYNCIO_MAX_TASKS`                 | Maximum concurrent asyncio tasks           |
| `ASGI_LIMIT`                        | Requests allowed per window (ASGI demo)    |
| `ASGI_WINDOW`                       | Window duration in seconds (ASGI demo)     |

## File Overview

| File | Purpose |
|------|---------|
| [config.py](config.py) | Central configuration for all demos |
| [threadpool/demo.py](threadpool/demo.py) | ThreadPool backend demo entry point |
| [celery/demo.py](celery/demo.py) | Celery backend demo entry point |
| [asyncio/demo.py](asyncio/demo.py) | AsyncIO backend demo entry point |
| [asgi/demo.py](asgi/demo.py) | ASGI/FastAPI middleware demo |
| [runner.py](runner.py) | Shared demo sequence (dedup, burst, monitoring, cleanup) |
| [tasks.py](tasks.py) | Mock workload functions (sync and async variants) |
| [dashboard.py](dashboard.py) | Live terminal UI that visualises limiter state |
