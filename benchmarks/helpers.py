"""Shared helpers for benchmark tests."""

import gc
import json
import time
import urllib.error
import urllib.request


class GCTracker:
    """Records GC pause durations via ``gc.callbacks``.

    Use as a context manager around the measurement region::

        with GCTracker(start_mono) as tracker:
            ...
        gc_stats = tracker.stats()
    """

    def __init__(self, start_mono):
        self._start_mono = start_mono
        self._phase_start = 0.0
        self.pauses = []

    def _callback(self, phase, info):
        if phase == "start":
            self._phase_start = time.monotonic()
        elif phase == "stop":
            duration = time.monotonic() - self._phase_start
            elapsed = self._phase_start - self._start_mono
            self.pauses.append(
                (round(elapsed, 3), info["generation"], round(duration * 1000, 3))
            )

    def __enter__(self):
        gc.callbacks.append(self._callback)
        return self

    def __exit__(self, *exc):
        gc.callbacks.remove(self._callback)
        return False

    def stats(self):
        if not self.pauses:
            return {
                "gc_pause_count": 0,
                "gc_total_ms": 0.0,
                "gc_max_ms": 0.0,
                "gc_pauses": [],
            }
        durations = [p[2] for p in self.pauses]
        return {
            "gc_pause_count": len(self.pauses),
            "gc_total_ms": round(sum(durations), 3),
            "gc_max_ms": round(max(durations), 3),
            "gc_pauses": self.pauses,
        }


class ToxiproxyHelper:
    """Minimal Toxiproxy REST API client using stdlib ``urllib``."""

    def __init__(self, api_url):
        self._api_url = api_url.rstrip("/")

    def _request(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"{self._api_url}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"} if data else {},
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read()) if resp.status != 204 else None
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"Toxiproxy {method} {path} failed: {e.code} {e.read().decode()}"
            ) from e

    def create_proxy(self, name, listen, upstream):
        return self._request(
            "POST",
            "/proxies",
            {
                "name": name,
                "listen": listen,
                "upstream": upstream,
                "enabled": True,
            },
        )

    def delete_proxy(self, name):
        self._request("DELETE", f"/proxies/{name}")

    def set_enabled(self, name, enabled):
        return self._request("PATCH", f"/proxies/{name}", {"enabled": enabled})

    def add_toxic(
        self, proxy_name, toxic_name, toxic_type, stream="downstream", **attributes
    ):
        return self._request(
            "POST",
            f"/proxies/{proxy_name}/toxics",
            {
                "name": toxic_name,
                "type": toxic_type,
                "stream": stream,
                "toxicity": 1.0,
                "attributes": attributes,
            },
        )

    def remove_toxic(self, proxy_name, toxic_name):
        self._request("DELETE", f"/proxies/{proxy_name}/toxics/{toxic_name}")

    def reset(self, proxy_name):
        """Remove all toxics and re-enable the proxy."""
        proxy = self._request("GET", f"/proxies/{proxy_name}")
        for toxic in proxy.get("toxics", []):
            self.remove_toxic(proxy_name, toxic["name"])
        self.set_enabled(proxy_name, True)

    def is_reachable(self):
        try:
            self._request("GET", "/version")
            return True
        except (OSError, RuntimeError):
            return False


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
