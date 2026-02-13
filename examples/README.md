# Examples

Two self-contained demos that showcase the rate limiter with different backends. Both run the same sequence: a **deduplication test**, an **error-recovery test** (tasks that raise exceptions to show that concurrency slots are released and processing continues), and a **burst test**, all displayed via a live terminal dashboard.

## Prerequisites

A running Redis instance is required. Start one with Docker:

```bash
docker-compose up redis          # exposes Redis on port 6380
```

Or use a local Redis on the default port (6379).

## Running the demos

### ThreadPool demo

Runs everything in a single Python process using a `ThreadPoolExecutor`. Fastest way to see the rate limiter in action.

```bash
# Local Redis (port 6379)
poetry run python -m examples.threadpool.demo

# Docker Redis (port 6380)
REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.threadpool.demo
```

### Celery demo

Spawns a Celery worker subprocess automatically and coordinates tasks over Redis. Demonstrates distributed rate limiting across processes.

```bash
# Local Redis (port 6379)
poetry run python -m examples.celery.demo

# Docker Redis (port 6380)
REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.celery.demo
```

The worker takes ~3 seconds to connect before the demo begins.

## Environment variables

| Variable     | Default     | Description                  |
|--------------|-------------|------------------------------|
| `REDIS_HOST` | `localhost` | Redis hostname               |
| `REDIS_PORT` | `6379`      | Redis port                   |

## Configuration

All tuneable parameters live in [config.py](config.py):

| Parameter                           | Description                              |
|-------------------------------------|------------------------------------------|
| `REDIS_HOST`                        | Redis hostname (env override supported)  |
| `REDIS_PORT`                        | Redis port (env override supported)      |
| `LIMIT`                             | Tokens allowed per window                |
| `WINDOW`                            | Window duration in seconds               |
| `MAX_CONCURRENCY`                   | Max tasks executing concurrently         |
| `TASK_SLEEP_MIN`                    | Min simulated API latency (seconds)      |
| `TASK_SLEEP_MAX`                    | Max simulated API latency (seconds)      |
| `DEDUP_COUNT`                       | Identical tasks in the deduplication test |
| `ERROR_COUNT`                       | Tasks that raise an exception (error-recovery test) |
| `BURST_COUNT`                       | Unique tasks queued in the burst test    |
| `PRIORITY_SEED`                     | Seed for reproducible random task priorities |
| `THREADPOOL_MAX_WORKERS`            | Thread pool size                         |
| `CELERY_WORKER_PREFETCH_MULTIPLIER` | Tasks fetched per Celery worker at a time |

## File overview

| File | Purpose |
|------|---------|
| [config.py](config.py) | Central configuration for all demos |
| [threadpool/demo.py](threadpool/demo.py) | ThreadPool backend demo entry point |
| [celery/demo.py](celery/demo.py) | Celery backend demo entry point |
| [runner.py](runner.py) | Shared demo sequence (dedup, burst, monitoring, cleanup) |
| [tasks.py](tasks.py) | Mock `mock_api_call` function used as the workload |
| [dashboard.py](dashboard.py) | Live terminal UI that visualises limiter state |
