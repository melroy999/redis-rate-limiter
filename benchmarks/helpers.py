"""Shared helpers for benchmark tests."""

import json
import time


def bulk_fill_buffer(
    limiter,
    count: int,
    start_id: int = 0,
    func_path: str | None = None,
    payload: dict | None = None,
) -> None:
    """Insert ``count`` minimal tasks into the buffer in a single ZADD call."""
    if count <= 0:
        return
    now_ms = int(time.time() * 1000)

    def _task(i: int) -> dict:
        task: dict = {"id": f"fill_{start_id + i}", "__meta_arrived_at": now_ms}
        if func_path is not None:
            task["func_path"] = func_path
            task["payload"] = payload if payload is not None else {}
        return task

    mapping = {json.dumps(_task(i)): 100 for i in range(count)}
    limiter.redis.zadd(limiter.buffer_key, mapping)
