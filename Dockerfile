# --- Stage 1: Base Python ---
FROM python:3.12-slim AS base

# Configure poetry to not create a virtualenv inside the container,
# given that the docker image is already an isolated environment.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=2.3.1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

RUN pip install "poetry==$POETRY_VERSION"

# Copy over the files containing dependency data.
COPY pyproject.toml poetry.lock ./

# --- Stage 2: Tests ---
FROM base AS test

# Install redis.
RUN apt-get update && \
    apt-get install -y redis-server

# Include a --no-root variant for efficient caching--this avoids redownloads of packages.
RUN poetry install --no-interaction --no-ansi --no-root

# Copy source and tests.
COPY src/ ./src/
COPY tests/ ./tests/

# Install all dependencies.
RUN poetry install --no-interaction --no-ansi

# Start the redis server and run the tests.
# If any tests fail, the build process stops here.
RUN redis-server --daemonize yes && \
    sleep 2 && \
    PYTHONPATH=src python3 -m pytest tests/ && \
    redis-cli shutdown

# --- Stage 3: Production --- #
FROM base AS production

# Include a --no-root variant for efficient caching--this avoids redownloads of packages.
RUN poetry install --only main --no-interaction --no-ansi --no-root

# Copy only the source code.
# This is done from the test stage, such that it actually runs.
COPY --from=test /app/src ./src

# Install only the main dependencies.
RUN poetry install --only main --no-interaction --no-ansi

# TODO: configure the entrypoint. For now, we start the celery worker.
CMD ["celery", "-A", "src.worker_init", "worker", "-c", "10", "--loglevel=info"]
