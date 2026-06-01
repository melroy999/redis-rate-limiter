"""Buffer growth under write pressure benchmark.

Measures drain throughput while a feeder thread continuously writes
tasks faster than the drainer can consume, causing the buffer ZSET to
grow throughout the run. Throughput and buffer depth are sampled in
0.5-second time bins.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from statistics import mean
from uuid import uuid4

import redis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT
from benchmarks.helpers import bulk_fill_buffer
from benchmarks.soak_collector import _DispatchCounter
from redis_rate_limiter import ThreadPoolRateLimiter

DURATION = 10.0
BIN_INTERVAL = 0.5
FEEDER_BATCH = 2000
FEEDER_INTERVAL = 0.05
INITIAL_FILL = 5000

FUNC_PATH = "benchmarks.test_buffer_growth._noop"


def _noop(**kwargs):
    pass


def _feeder_loop(
    feeder_redis,
    limiter,
    stop_event,
):
    seq = INITIAL_FILL
    while not stop_event.wait(FEEDER_INTERVAL):
        bulk_fill_buffer(
            limiter,
            FEEDER_BATCH,
            start_id=seq,
            func_path=FUNC_PATH,
            redis_client=feeder_redis,
        )
        seq += FEEDER_BATCH


def test_buffer_growth_under_write_pressure(
    redis_client,
    request,
):
    """Measure drain throughput while buffer grows from continuous writes."""
    # Arrange
    redis_client.flushdb()
    limiter_id = f"buffer_growth_{uuid4().hex[:12]}"

    executor = ThreadPoolExecutor(max_workers=8)
    barrier = threading.Barrier(8)
    futures = [executor.submit(barrier.wait) for _ in range(8)]
    for f in futures:
        f.result(timeout=5.0)

    counter = _DispatchCounter()
    limiter = ThreadPoolRateLimiter(
        redis_client=redis_client,
        executor=executor,
        _sentinel=ThreadPoolRateLimiter._SENTINEL,
        limiter_id=limiter_id,
        metrics_callback=counter,
        limit=10_000,
        window=1.0,
        max_concurrency=10_000_000,
        max_age=3600,
        lease_duration=30,
        jitter_enabled=False,
    )

    feeder_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    bulk_fill_buffer(
        limiter, INITIAL_FILL, func_path=FUNC_PATH, redis_client=feeder_redis
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

    bins = []
    num_bins = int(DURATION / BIN_INTERVAL)
    for i in range(num_bins):
        t_start = time.monotonic()
        count_start = counter.count
        depth_start = feeder_redis.zcard(limiter.buffer_key)

        time.sleep(BIN_INTERVAL)

        t_end = time.monotonic()
        count_end = counter.count
        elapsed = t_end - t_start

        throughput = (count_end - count_start) / elapsed if elapsed > 0 else 0.0
        bins.append(
            {
                "elapsed_s": round((i + 1) * BIN_INTERVAL, 1),
                "throughput": round(throughput, 1),
                "buffer_depth": depth_start,
            }
        )

    feeder_stop.set()
    feeder_thread.join(timeout=5.0)

    limiter.shutdown()
    executor.shutdown(wait=True)
    feeder_redis.close()

    # Assert
    assert len(bins) > 0, "no measurement bins were collected"
    assert bins[-1]["buffer_depth"] > bins[0]["buffer_depth"], (
        "buffer did not grow, feeder was too slow"
    )

    mid = len(bins) // 2
    first_half = mean(b["throughput"] for b in bins[:mid])
    second_half = mean(b["throughput"] for b in bins[mid:])
    ratio = second_half / first_half if first_half > 0 else 0.0
    assert ratio >= 0.70, f"throughput degraded by more than 30%: ratio={ratio:.2f}"

    request.node.user_properties.append(("buffer_growth_result", {"bins": bins}))
