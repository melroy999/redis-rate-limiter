"""Entry point for the K8s/Docker Compose demonstration.

Delegates to either the traffic generator or worker mode based on the
``TRAFFIC_GENERATOR`` environment variable.

Usage::

    python -m demo                            # worker mode (default)
    TRAFFIC_GENERATOR=true python -m demo     # generator mode
"""

import logging
import math
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import redis
from prometheus_client import Counter, Gauge, start_http_server

from demo import config
from demo.tasks import FUNC_PATH
from redis_rate_limiter import PrometheusMetricsExporter, ThreadPoolRateLimiter

logger = logging.getLogger("demo")


# ---------------------------------------------------------------------------
# Shared setup
# ---------------------------------------------------------------------------


def _setup_logging() -> None:
    """Configure console logging for the demonstration."""
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("redis_rate_limiter").setLevel(logging.WARNING)


def _connect_redis() -> redis.Redis:
    """Establish a connection to Redis, retrying until available."""
    client = redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        decode_responses=True,
    )
    for attempt in range(30):
        try:
            client.ping()
            return client
        except redis.ConnectionError:
            logger.info(
                "Waiting for Redis (%s:%d)...", config.REDIS_HOST, config.REDIS_PORT
            )
            time.sleep(1)
    raise RuntimeError("Could not connect to Redis after 30 attempts.")


def _create_limiter(
    redis_client: redis.Redis,
    executor: ThreadPoolExecutor,
    exporter: PrometheusMetricsExporter,
) -> ThreadPoolRateLimiter:
    """Create and configure the shared rate limiter instance."""
    ThreadPoolRateLimiter.configure(redis_client, executor=executor)
    return ThreadPoolRateLimiter.create(
        limiter_id=config.LIMITER_ID,
        limit=config.LIMIT,
        window=config.WINDOW,
        max_concurrency=config.MAX_CONCURRENCY,
        override=True,
        metrics_callback=exporter,
        drain_enabled=not config.IS_TRAFFIC_GENERATOR,
    )


# ---------------------------------------------------------------------------
# Traffic generator
# ---------------------------------------------------------------------------


def _run_traffic_generator(limiter: ThreadPoolRateLimiter) -> None:
    """Schedule tasks following a sine-wave pattern in a background thread.

    The offered rate is computed as::

        rate(t) = (LIMIT / WINDOW) * (CENTER + AMPLITUDE * sin(2 * pi * t / PERIOD))

    With the default configuration this gives a per-second rate that oscillates
    between 2.5 and 35, with an average of 18.75 (well below the 25/sec limit).
    """
    offered_rate_gauge = Gauge(
        "redis_rate_limiter_demo_offered_rate",
        "Current offered traffic rate in tasks per second.",
        ["limiter_id"],
    )

    # Counter for actual schedule_task() calls. Using rate() on this in
    # Grafana gives the real production rate, which may lag behind the
    # theoretical offered rate due to Redis round-trip latency.
    tasks_scheduled_counter = Counter(
        "redis_rate_limiter_demo_tasks_scheduled_total",
        "Total number of tasks scheduled by the traffic generator.",
        ["limiter_id"],
    )

    effective_rate = config.LIMIT / config.WINDOW
    seq = 0
    t0 = time.monotonic()

    while not _shutdown_event.is_set():
        loop_start = time.monotonic()
        elapsed = loop_start - t0
        rate = effective_rate * (
            config.SINE_CENTER
            + config.SINE_AMPLITUDE
            * math.sin(2 * math.pi * elapsed / config.SINE_PERIOD)
        )
        rate = max(0.1, rate)
        offered_rate_gauge.labels(limiter_id=config.LIMITER_ID).set(rate)

        limiter.schedule_task(FUNC_PATH, {"seq": seq})
        tasks_scheduled_counter.labels(limiter_id=config.LIMITER_ID).inc()
        seq += 1

        # Subtract time already spent (Redis round-trip, etc.) from the
        # sleep interval so the actual scheduling rate matches the target.
        remaining = (1.0 / rate) - (time.monotonic() - loop_start)
        if remaining > 0:
            _shutdown_event.wait(remaining)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

_shutdown_event = threading.Event()


def _handle_signal(signum: int, _frame) -> None:
    sig_name = signal.Signals(signum).name
    logger.info("Received %s, shutting down...", sig_name)
    _shutdown_event.set()


def main() -> None:
    _setup_logging()
    mode = "generator" if config.IS_TRAFFIC_GENERATOR else "worker"
    pod_name = os.getenv("HOSTNAME", "local")
    logger.info(
        "Starting demo in %s mode: pod=%s, limit=%d, window=%.1fs, concurrency=%d.",
        mode,
        pod_name,
        config.LIMIT,
        config.WINDOW,
        config.MAX_CONCURRENCY,
    )

    redis_client = _connect_redis()
    executor = ThreadPoolExecutor(max_workers=5)

    exporter = PrometheusMetricsExporter(limiter_id=config.LIMITER_ID)

    # Expose the configured rate limit as a Prometheus gauge so the Grafana
    # dashboard can draw a dynamic threshold line.
    effective_rate_gauge = Gauge(
        "redis_rate_limiter_demo_effective_rate",
        "Configured rate limit in tasks per second (limit / window).",
        ["limiter_id"],
    )
    effective_rate_gauge.labels(limiter_id=config.LIMITER_ID).set(
        config.LIMIT / config.WINDOW,
    )

    start_http_server(config.METRICS_PORT)
    logger.info("Prometheus metrics server started on port %d.", config.METRICS_PORT)

    limiter = _create_limiter(redis_client, executor, exporter)
    logger.info("Rate limiter created: id=%s.", config.LIMITER_ID)

    # Register signal handlers for graceful shutdown.
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if config.IS_TRAFFIC_GENERATOR:
        logger.info(
            "Traffic generator active: period=%.0fs, center=%.2f, amplitude=%.2f.",
            config.SINE_PERIOD,
            config.SINE_CENTER,
            config.SINE_AMPLITUDE,
        )
        generator_thread = threading.Thread(
            target=_run_traffic_generator,
            args=(limiter,),
            daemon=True,
        )
        generator_thread.start()
    else:
        # Workers do not call schedule_task(), so their drain loops are never
        # started automatically. Kick-start them with periodic triggers until
        # the loop becomes self-sustaining (after the first successful drain).
        def _kickstart_drain() -> None:
            for _ in range(30):
                limiter.trigger_consume()
                if _shutdown_event.wait(1):
                    return

        threading.Thread(target=_kickstart_drain, daemon=True).start()

    # Block until a shutdown signal is received.
    _shutdown_event.wait()

    logger.info("Shutting down limiter and executor...")
    limiter.shutdown()
    executor.shutdown(wait=False)
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
