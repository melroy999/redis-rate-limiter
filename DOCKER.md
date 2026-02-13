# Docker and Docker Compose Guide

This document describes how to use Docker and Docker Compose with the celery-rate-limiter project.

## Quick Start

### Option 1: Using Docker Compose Redis (Recommended)

Start Redis in Docker, which avoids conflicts with a local Redis installation:

```bash
# Start Redis on port 6380 (avoids conflicts with local Redis on 6379).
docker-compose up redis

# Run tests locally, connecting to Docker Redis.
REDIS_HOST=localhost REDIS_PORT=6380 poetry run pytest

# Or run tests inside Docker.
docker-compose --profile test up test
```

### Option 2: Using Local Redis

If Redis is already running locally, the tests will connect to it automatically:

```bash
# Tests will use localhost:6379 by default.
poetry run pytest
```

## Docker Compose Services

### 1. redis - Redis Backend

The Redis service that stores the rate limiting data.

**Default configuration:**

- *Internal port*: 6379 (inside the Docker network).
- *External port*: 6380 (mapped to localhost to avoid conflicts).
- *Data persistence*: enabled via the `redis-data` volume.

**Usage:**

```bash
# Start Redis only.
docker-compose up redis

# Start in background.
docker-compose up -d redis

# Stop Redis.
docker-compose down
```

**Custom port mapping:**

If port 6380 is also in use, the port can be overridden with an environment variable:

```bash
# Use port 6381 instead.
REDIS_PORT=6381 docker-compose up redis
```

Alternatively, a `.env` file can be created:

```bash
cp .env.example .env
# Edit .env and set REDIS_PORT=6381.
docker-compose up redis
```

### 2. test - Test Runner

Runs the test suite inside Docker with a clean Redis instance.

**Usage:**

```bash
# Run all tests.
docker-compose --profile test up test

# Run specific tests.
docker-compose --profile test run test poetry run pytest tests/implementations/ -v

# Run tests without capturing output (shows print statements).
docker-compose --profile test run test poetry run pytest tests/ -v -s

# Interactive shell in the test container.
docker-compose --profile test run test bash
```

**Benefits:**

- Provides an isolated test environment.
- Eliminates the need to install Redis locally.
- Ensures consistency across different machines.
- Print statements from tests are visible (uses the `-s` flag).

### 3. celery-worker - Production Worker

Runs Celery workers in production mode.

**Usage:**

```bash
# Start Redis and the Celery workers.
docker-compose --profile production up

# Scale the number of workers.
docker-compose --profile production up --scale celery-worker=3
```

### 4. redis-commander - Redis Web UI (Optional)

A web-based UI for debugging the Redis data.

**Usage:**

```bash
# Start Redis and Redis Commander.
docker-compose --profile debug up redis redis-commander

# Access at http://localhost:8081.
```

## Common Scenarios

### Scenario 1: Local Development with Local Redis

**Situation:** Redis is installed and running locally on port 6379.

**Solution:** The tests will connect automatically without any additional configuration.

```bash
# Start local Redis (if not running).
redis-server

# Run tests.
poetry run pytest
```

### Scenario 2: Local Development with Docker Redis

**Situation:** Docker Redis is preferred, but the tests should be run locally.

**Solution:** Start Docker Redis and point the tests to it.

```bash
# Terminal 1: Start Docker Redis.
docker-compose up redis

# Terminal 2: Run tests with Docker Redis.
REDIS_HOST=localhost REDIS_PORT=6380 poetry run pytest
```

**Tip:** The environment variables can be added to the shell profile for convenience:

```bash
# Add to ~/.bashrc or ~/.zshrc.
export REDIS_HOST=localhost
export REDIS_PORT=6380
```

### Scenario 3: Port Conflict on 6380

**Situation:** Both ports 6379 and 6380 are in use.

**Solution:** Use a different port.

```bash
# Start Redis on port 6381.
REDIS_PORT=6381 docker-compose up redis

# Run tests with the custom port.
REDIS_HOST=localhost REDIS_PORT=6381 poetry run pytest
```

### Scenario 4: Running Everything in Docker

**Situation:** Complete isolation is desired, i.e., no local dependencies.

**Solution:** Use Docker Compose profiles.

```bash
# Run tests in Docker (no local setup needed).
docker-compose --profile test up test

# Run the production stack.
docker-compose --profile production up
```

### Scenario 5: CI/CD Pipeline

**Situation:** Running tests in GitHub Actions or other CI systems.

**Solution:** Use Docker Compose in CI.

```yaml
# Example GitHub Actions workflow.
- name: Start Redis
  run: docker-compose up -d redis

- name: Wait for Redis
  run: docker-compose exec -T redis redis-cli ping

- name: Run Tests
  run: poetry run pytest
  env:
    REDIS_HOST: localhost
    REDIS_PORT: 6380
```

## Troubleshooting

### Tests Fail: "Could not connect to Redis"

**Problem:** The tests cannot connect to Redis.

**Solutions:**

1. Check if Redis is running:
   ```bash
   # For local Redis.
   redis-cli ping

   # For Docker Redis.
   docker-compose exec redis redis-cli ping
   ```

2. Check the port configuration:
   ```bash
   # See what is running on port 6379.
   lsof -i :6379

   # See what is running on port 6380.
   lsof -i :6380
   ```

3. Verify the environment variables:
   ```bash
   echo $REDIS_HOST
   echo $REDIS_PORT
   ```

### Port Already Allocated

**Problem:** `Error starting userland proxy: listen tcp4 0.0.0.0:6380: bind: address already in use`.

**Solution:** Use a different port:

```bash
REDIS_PORT=6381 docker-compose up redis
```

### Docker Redis Will Not Start

**Problem:** The Redis container fails to start.

**Solutions:**

1. Check the Docker logs:
   ```bash
   docker-compose logs redis
   ```

2. Remove old containers:
   ```bash
   docker-compose down
   docker-compose up redis
   ```

3. Remove volumes (**warning**: this deletes all Redis data):
   ```bash
   docker-compose down -v
   docker-compose up redis
   ```

## Best Practices

### For Development

1. **Use Docker Redis**: avoids version mismatches and configuration drift.
   ```bash
   docker-compose up -d redis
   ```

2. **Set environment variables**: make the connection permanent.
   ```bash
   echo "export REDIS_PORT=6380" >> ~/.bashrc
   ```

3. **Use Redis Commander**: allows for visual debugging of the rate limiting behavior.
   ```bash
   docker-compose --profile debug up -d redis redis-commander
   ```

### For Testing

1. **Use the Docker test runner**: ensures a clean environment.
   ```bash
   docker-compose --profile test up test
   ```

2. **Flush Redis between test runs**: keeps the tests independent.
   ```bash
   # Tests do this automatically, but for manual testing:
   docker-compose exec redis redis-cli FLUSHALL
   ```

### For Production

1. **Use the production profile**: provides an optimized build.
   ```bash
   docker-compose --profile production up -d
   ```

2. **Persist Redis data**: ensure the `redis-data` volume is backed up.

3. **Monitor health**: use the built-in health checks.
   ```bash
   docker-compose ps
   ```

## Architecture

### Network Topology

The Docker Compose setup creates an isolated network where services communicate internally:

- Redis runs on port 6379 inside the Docker network.
- Redis is mapped to port 6380 on localhost to avoid conflicts.
- Celery workers connect to `redis:6379` via internal Docker networking.
- Local tests can connect to `localhost:6380`.
- The configuration is controlled via environment variables.

This approach provides the following benefits:

- Local and Docker Redis can run simultaneously.
- Switching between local and Docker Redis is straightforward.
- There are no conflicts with existing services.
- Internal Docker networking remains consistent.

### Port Mapping Strategy

- **Inside Docker:** all services use the standard port 6379.
- **Outside Docker:** mapped to 6380 to avoid conflicts.
- **Configurable:** the port can be overridden with the `REDIS_PORT` environment variable.

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis hostname (for tests). |
| `REDIS_PORT` | `6379` (tests), `6380` (Docker) | Redis port. |
| `CELERY_BROKER_URL` | Auto-configured | Celery broker URL. |
| `CELERY_RESULT_BACKEND` | Auto-configured | Celery result backend URL. |

## Further Reading

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Redis Docker Hub](https://hub.docker.com/_/redis)
- [Celery Documentation](https://docs.celeryproject.org/)
