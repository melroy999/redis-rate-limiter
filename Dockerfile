# --- Stage 1: Base Python ---
FROM python:3.12-slim AS base

# Ensure vunerabilities are patched.
RUN apt-get update && apt-get upgrade -y && apt-get clean

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
# Set up the environment only. Tests are executed by our CI workflow.
FROM base AS test

# Install redis.
RUN apt-get update && \
    apt-get install -y redis-server

# Caching.
RUN poetry install --no-interaction --no-ansi --no-root

# Copy source and tests.
COPY src/ ./src/
COPY tests/ ./tests/

# Install all dependencies.
RUN poetry install --no-interaction --no-ansi

# --- Stage 3: Production ---
FROM base AS production

# Caching.
RUN poetry install --only main --no-interaction --no-ansi --no-root

# Copy source folder only.
COPY src/ ./src/

# Install only the main dependencies.
RUN poetry install --only main --no-interaction --no-ansi

# Start celery.
CMD ["celery", "-A", "src.worker_init", "worker", "-c", "10", "--loglevel=info"]
