"""Central configuration for the K8s/Docker Compose demonstration.

All values are read from environment variables with sensible defaults for
container-based deployments (e.g., ``REDIS_HOST`` defaults to ``redis``,
the Docker Compose / K8s service name).
"""

import os

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

LIMITER_ID: str = os.getenv("LIMITER_ID", "k8s-demo")
LIMIT: int = int(os.getenv("LIMIT", "250"))
WINDOW: float = float(os.getenv("WINDOW", "10.0"))
MAX_CONCURRENCY: int = int(os.getenv("MAX_CONCURRENCY", "9"))

# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------

METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8000"))

# ---------------------------------------------------------------------------
# Traffic generator
# ---------------------------------------------------------------------------

IS_TRAFFIC_GENERATOR: bool = os.getenv("TRAFFIC_GENERATOR", "false").lower() == "true"

# Sine wave parameters that control the offered load pattern.
# rate(t) = (LIMIT / WINDOW) * (SINE_CENTER + SINE_AMPLITUDE * sin(2 * pi * t / SINE_PERIOD))
#
# With the defaults below, the effective per-second rate ranges from
# (0.75 - 0.65) * 25 = 2.5 to (0.75 + 0.65) * 25 = 35 tasks/sec.
# The average rate (18.75/sec) is well below the 25/sec limit, giving the
# buffer enough headroom to fully drain between peaks.
SINE_PERIOD: float = float(os.getenv("SINE_PERIOD", "120"))
SINE_AMPLITUDE: float = float(os.getenv("SINE_AMPLITUDE", "0.65"))
SINE_CENTER: float = float(os.getenv("SINE_CENTER", "0.75"))

# ---------------------------------------------------------------------------
# Mock task
# ---------------------------------------------------------------------------

TASK_DURATION: float = float(os.getenv("TASK_DURATION", "0.3"))
