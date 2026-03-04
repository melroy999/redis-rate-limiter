# Demo image for the K8s/Docker Compose rate limiter demonstration.
#
# Build context must be the project root:
#   docker build -f demo/Dockerfile -t redis-rate-limiter-demo .

ARG PYTHON_VERSION=3.12
ARG POETRY_VERSION=2.3.1

FROM python:${PYTHON_VERSION}-slim AS base

ARG POETRY_VERSION

RUN apt-get update && apt-get upgrade -y && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

# Install dependencies (cached layer).
COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-interaction --no-ansi --no-root -E prometheus

# Copy source and demo code.
COPY src/ ./src/
COPY demo/ ./demo/

# Install the library itself.
RUN poetry install --only main --no-interaction --no-ansi -E prometheus

# Create a non-root user for security hardening.
RUN useradd -m -u 1000 demo && chown -R demo:demo /app
USER demo

EXPOSE 8000

ENTRYPOINT ["python", "-m", "demo"]
