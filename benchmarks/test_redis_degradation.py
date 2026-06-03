"""Redis degradation benchmark.

Measures how the rate limiter degrades and recovers under adverse
network conditions injected via Toxiproxy at the TCP layer. A single
test runs four sequential phases:

- **baseline**: clean connection, steady-state throughput reference.
- **degraded**: 15ms latency + 5ms jitter on all Redis traffic.
- **partition**: proxy disabled, all connections dropped.
- **recovery**: proxy re-enabled, toxics removed (15s to allow
  full watchdog cycle + reconnection).

Throughput is sampled in 0.5-second time bins throughout. The phase
transitions show backoff behavior, failure handling, and recovery
speed.

Requires the ``benchmark-degraded`` Docker Compose profile which
starts Toxiproxy between the benchmark container and Redis. Skipped
automatically when Toxiproxy is not reachable.
"""

import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
import redis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT
from benchmarks.helpers import GCTracker, ToxiproxyHelper, bulk_fill_buffer
from benchmarks.soak_collector import _DispatchCounter
from redis_rate_limiter import ThreadPoolRateLimiter

TOXIPROXY_API = os.getenv("TOXIPROXY_API", "")
TOXIPROXY_UPSTREAM = os.getenv("TOXIPROXY_UPSTREAM", "redis:6379")
PROXY_NAME = "redis"
PROXY_LISTEN = "0.0.0.0:16379"

BIN_INTERVAL = 0.5
BUFFER_WATERMARK = 1500
FEEDER_INTERVAL = 0.2
FEEDER_BATCH = 200
FUNC_PATH = "benchmarks.test_redis_degradation._noop"

PHASES = [
    ("baseline", 10.0, None),
    ("degraded", 10.0, {"type": "latency", "latency": 15, "jitter": 5}),
    ("partition", 3.0, "disable"),
    ("recovery", 15.0, None),
]

LIMIT = 500
WINDOW = 1.0
MAX_CONCURRENCY = 10_000_000


def _noop(**kwargs):
    pass


def _feeder_loop(feeder_redis, limiter, stop_event):
    seq = 0
    while not stop_event.wait(FEEDER_INTERVAL):
        try:
            current = feeder_redis.zcard(limiter.buffer_key)
            if current < BUFFER_WATERMARK:
                deficit = BUFFER_WATERMARK - current + FEEDER_BATCH
                bulk_fill_buffer(
                    limiter,
                    deficit,
                    start_id=seq,
                    func_path=FUNC_PATH,
                    redis_client=feeder_redis,
                )
                seq += deficit
        except redis.RedisError:
            pass


def _measure_bins(counter, duration):
    bins = []
    num_bins = int(duration / BIN_INTERVAL)
    for i in range(num_bins):
        t_start = time.monotonic()
        count_start = counter.count

        time.sleep(BIN_INTERVAL)

        t_end = time.monotonic()
        count_end = counter.count
        elapsed = t_end - t_start

        throughput = (count_end - count_start) / elapsed if elapsed > 0 else 0.0
        bins.append(
            {
                "elapsed_s": round((i + 1) * BIN_INTERVAL, 1),
                "throughput": round(throughput, 1),
            }
        )
    return bins


pytestmark = pytest.mark.skipif(
    not TOXIPROXY_API,
    reason="TOXIPROXY_API not set; run with benchmark-degraded profile",
)


@pytest.fixture(scope="module")
def toxiproxy():
    helper = ToxiproxyHelper(TOXIPROXY_API)
    if not helper.is_reachable():
        pytest.skip("Toxiproxy not reachable")
    helper.create_proxy(PROXY_NAME, PROXY_LISTEN, TOXIPROXY_UPSTREAM)
    yield helper
    helper.delete_proxy(PROXY_NAME)


def test_redis_degradation(request, toxiproxy):
    """Measure limiter throughput across healthy, degraded, partitioned, and recovery phases."""
    # Arrange
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    redis_client.flushdb()
    limiter_id = f"degrade_{uuid4().hex[:8]}"

    executor = ThreadPoolExecutor(max_workers=8)
    warmup_barrier = threading.Barrier(8)
    futures = [executor.submit(warmup_barrier.wait) for _ in range(8)]
    for f in futures:
        f.result(timeout=5.0)

    counter = _DispatchCounter()
    limiter = ThreadPoolRateLimiter(
        redis_client=redis_client,
        executor=executor,
        _sentinel=ThreadPoolRateLimiter._SENTINEL,
        limiter_id=limiter_id,
        metrics_callback=counter,
        limit=LIMIT,
        window=WINDOW,
        max_concurrency=MAX_CONCURRENCY,
        max_age=3600,
        lease_duration=30,
        jitter_enabled=False,
    )

    feeder_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    bulk_fill_buffer(
        limiter, BUFFER_WATERMARK, func_path=FUNC_PATH, redis_client=feeder_redis
    )

    feeder_stop = threading.Event()
    feeder_thread = threading.Thread(
        target=_feeder_loop,
        args=(feeder_redis, limiter, feeder_stop),
        daemon=True,
    )

    # Act
    feeder_thread.start()
    limiter.trigger_consume()

    all_bins = []
    phase_markers = []
    cumulative_elapsed = 0.0

    with GCTracker(time.monotonic()) as gc_tracker:
        for phase_name, duration, condition in PHASES:
            sys.stderr.write(f"\n[degradation/{phase_name}] starting ({duration}s)\n")
            sys.stderr.flush()

            if condition == "disable":
                toxiproxy.set_enabled(PROXY_NAME, False)
            elif condition is not None:
                toxic_type = condition["type"]
                attrs = {k: v for k, v in condition.items() if k != "type"}
                toxiproxy.add_toxic(
                    PROXY_NAME, f"toxic_{phase_name}", toxic_type, **attrs
                )
            else:
                toxiproxy.reset(PROXY_NAME)

            phase_markers.append(
                {
                    "phase": phase_name,
                    "start_s": round(cumulative_elapsed, 1),
                    "duration": duration,
                    "condition": str(condition) if condition else "healthy",
                }
            )

            bins = _measure_bins(counter, duration)
            for b in bins:
                b["elapsed_s"] = round(cumulative_elapsed + b["elapsed_s"], 1)
                b["phase"] = phase_name
            all_bins.extend(bins)
            cumulative_elapsed += duration

    gc_result = gc_tracker.stats()

    feeder_stop.set()
    feeder_thread.join(timeout=5.0)
    limiter.shutdown()
    executor.shutdown(wait=True)
    feeder_redis.close()
    redis_client.close()

    # Assert
    assert len(all_bins) > 0, "no measurement bins were collected"

    request.node.user_properties.append(
        (
            "redis_degradation_result",
            {
                "bins": all_bins,
                "phases": phase_markers,
                "total_dispatches": counter.count,
                **gc_result,
            },
        )
    )
    request.node.user_properties.append(
        (
            "gc_summary_result",
            {"test": "redis_degradation", "scenario": "", **gc_result},
        )
    )
