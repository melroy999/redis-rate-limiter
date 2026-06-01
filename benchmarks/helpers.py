"""Shared helpers for benchmark tests."""

import json
import time


def bulk_fill_buffer(
    limiter,
    count,
    start_id=0,
    func_path=None,
    payload=None,
    redis_client=None,
):
    """Insert ``count`` minimal tasks into the buffer in a single ZADD call.

    ``redis_client`` overrides ``limiter.redis`` and is required when the
    limiter holds an async client (since this helper is sync).
    """
    if count <= 0:
        return
    now_ms = int(time.time() * 1000)

    def _task(i):
        task_id = f"fill_{start_id + i}"
        task = {
            "id": task_id,
            "inflight_key": limiter.get_inflight_key(task_id),
            "__meta_arrived_at": now_ms,
        }
        if func_path is not None:
            task["func_path"] = func_path
            task["payload"] = payload if payload is not None else {}
        return task

    mapping = {json.dumps(_task(i)): 100 for i in range(count)}
    client = redis_client if redis_client is not None else limiter.redis
    client.zadd(limiter.buffer_key, mapping)
