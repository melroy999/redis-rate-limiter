# --- Stage 1: Base Python ---
# Using build args allows easy version updates without editing multiple lines.
ARG PYTHON_VERSION=3.12
ARG POETRY_VERSION=2.3.1

FROM python:${PYTHON_VERSION}-slim AS base

# Re-declare ARG to make it available inside this build stage.
ARG POETRY_VERSION

# Ensure vulnerabilities are patched.
RUN apt-get update && apt-get upgrade -y && apt-get clean

# Configure poetry to not create a virtualenv inside the container,
# given that the docker image is already an isolated environment.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=${POETRY_VERSION} \
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
    apt-get install -y redis-server && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Caching.
RUN poetry install --no-interaction --no-ansi --no-root -E celery -E prometheus

# Copy source and tests.
COPY src/ ./src/
COPY tests/ ./tests/

# Install all dependencies (including the celery and prometheus extras for tests).
RUN poetry install --no-interaction --no-ansi -E celery -E prometheus

# --- Stage 3: Production ---
FROM base AS production

# Caching.
RUN poetry install --only main --no-interaction --no-ansi --no-root -E celery

# Copy source and examples.
COPY src/ ./src/
COPY examples/ ./examples/

# Install only the main dependencies (with celery extra for the worker).
RUN poetry install --only main --no-interaction --no-ansi -E celery

# Create a non-root user for security hardening.
# Running as non-root limits the impact of potential container escapes.
RUN useradd -m -u 1000 celery && chown -R celery:celery /app
USER celery

# Health check verifies the Celery worker is responsive.
# Allows orchestrators (Docker Compose, Kubernetes) to detect and restart unhealthy containers.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD celery -A examples.worker inspect ping -d celery@$HOSTNAME || exit 1

# Start celery.
CMD ["celery", "-A", "examples.worker", "worker", "-c", "10", "--loglevel=info"]
