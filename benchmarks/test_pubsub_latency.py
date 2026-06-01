"""Redis Pub/Sub latency microbenchmarks.

Measures Redis Pub/Sub propagation latency in isolation, independent of
limiter throughput. Two tests: round-trip (publish to get_message) and
publish-only (fire-and-forget baseline). Serves as a calibration metric
for interpreting contention benchmark regressions.
"""

from uuid import uuid4

import pytest
import redis

from benchmarks.conftest import REDIS_HOST, REDIS_PORT

pytestmark = pytest.mark.benchmark(group="pubsub")


class TestPubSubLatency:
    def test_pubsub_round_trip(self, benchmark, redis_client):
        """Measure round-trip latency: publish to get_message."""
        # Arrange
        channel = f"bench_pubsub_{uuid4().hex[:12]}"
        subscriber = redis_client.pubsub()
        subscriber.subscribe(channel)
        while True:
            msg = subscriber.get_message(timeout=1.0)
            if msg is not None and msg["type"] == "subscribe":
                break

        publisher = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

        def _measure():
            publisher.publish(channel, "signal")
            while True:
                msg = subscriber.get_message(timeout=1.0)
                if msg is not None and msg["type"] == "message":
                    return

        # Act
        benchmark.pedantic(_measure, rounds=500, warmup_rounds=20, iterations=1)

        # Cleanup
        subscriber.unsubscribe(channel)
        subscriber.close()
        publisher.close()

    def test_pubsub_publish_only(self, benchmark, redis_client):
        """Measure publish-only latency (fire-and-forget baseline)."""
        # Arrange
        channel = f"bench_pubsub_pub_{uuid4().hex[:12]}"

        def _publish():
            redis_client.publish(channel, "signal")

        # Act
        benchmark.pedantic(_publish, rounds=2000, warmup_rounds=20, iterations=1)
