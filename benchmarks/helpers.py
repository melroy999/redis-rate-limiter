"""Shared helpers for benchmark tests."""

import json
import time


def bulk_fill_buffer(limiter, count: int, start_id: int = 0) -> None:
    """Insert ``count`` minimal tasks into the buffer in a single ZADD call."""
    if count <= 0:
        return
    now_ms = int(time.time() * 1000)
    mapping = {
        json.dumps({"id": f"fill_{start_id + i}", "__meta_arrived_at": now_ms}): 100
        for i in range(count)
    }
    limiter.redis.zadd(limiter.buffer_key, mapping)
