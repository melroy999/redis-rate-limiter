"""Self-contained ASGI rate limiting middleware demonstration.

Starts a FastAPI application with per-client-IP rate limiting, fires a sequence
of HTTP requests to demonstrate the middleware behaviour, and prints formatted
results showing rate limit headers, 429 responses, health endpoint bypass, and
gradual recovery as the sliding window counter decays.

Usage (Docker Redis on 6380):
    REDIS_HOST=localhost REDIS_PORT=6380 poetry run python -m examples.asgi.demo

Usage (local Redis on 6379):
    poetry run python -m examples.asgi.demo
"""

import asyncio
import http.client
import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from celery_rate_limiter.backends.asgi import ASGIRateLimiter, by_client_ip
from examples.config import ASGI_LIMIT, ASGI_WINDOW, REDIS_HOST, REDIS_PORT

logger = logging.getLogger("examples.asgi_demo")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LIMITER_ID = "asgi_demo"
HOST = "127.0.0.1"
PORT = 8099
EXTRA_REQUESTS = 3


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def setup_logging() -> None:
    """Initialise log files for the demonstration output and limiter internals."""
    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    log_dir = os.path.dirname(__file__)

    demo_handler = logging.FileHandler(
        os.path.join(log_dir, "demo.log"),
        mode="w",
    )
    demo_handler.setLevel(logging.DEBUG)
    demo_handler.setFormatter(fmt)
    logging.getLogger("examples").addHandler(demo_handler)
    logging.getLogger("examples").setLevel(logging.DEBUG)

    limiter_handler = logging.FileHandler(
        os.path.join(log_dir, "limiter_debug.log"),
        mode="w",
    )
    limiter_handler.setLevel(logging.DEBUG)
    limiter_handler.setFormatter(fmt)
    logging.getLogger("celery_rate_limiter").addHandler(limiter_handler)
    logging.getLogger("celery_rate_limiter").setLevel(logging.DEBUG)


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure the ASGI rate limiter on startup and clean up on shutdown."""
    redis_client = redis.asyncio.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    await redis_client.ping()

    # Flush stale keys from any previous run.
    async for key in redis_client.scan_iter(f"{LIMITER_ID}:*"):
        await redis_client.delete(key)

    ASGIRateLimiter.configure(redis_client)
    limiter = await ASGIRateLimiter.create(
        limiter_id=LIMITER_ID,
        limit=ASGI_LIMIT,
        window=ASGI_WINDOW,
        override=True,
    )

    app.state.limiter = limiter
    app.state.redis = redis_client
    logger.info(
        "ASGI rate limiter initialized: limit=%d, window=%.1fs.",
        ASGI_LIMIT,
        ASGI_WINDOW,
    )

    yield

    await redis_client.aclose()
    ASGIRateLimiter._reset()
    logger.info("ASGI rate limiter shut down.")


app = FastAPI(title="Rate Limiter Demo", lifespan=lifespan)


@app.get("/")
async def index():
    """A simple endpoint protected by the rate limiting middleware."""
    return {"message": "Hello, world!", "status": "allowed"}


@app.get("/health")
async def health():
    """Health check endpoint (not rate limited, see key_func bypass)."""
    return {"status": "ok"}


def _key_func(scope):
    """Extract the client IP, but bypass rate limiting for the health endpoint."""
    path = scope.get("path", "")
    if path == "/health":
        return None
    return by_client_ip(scope)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Apply rate limiting as FastAPI middleware.

    This approach is used instead of wrapping the ASGI app directly so that
    the middleware can access ``app.state.limiter``, which is populated during
    the lifespan startup.
    """
    limiter = request.app.state.limiter
    key = _key_func(request.scope)

    if key is None:
        return await call_next(request)

    try:
        result = await limiter.acquire(key)
    except Exception:
        logger.exception("Rate limiter error; failing open.")
        return await call_next(request)

    if not result["allowed"]:
        retry_after = max(1, result["reset_in_ms"] // 1000)
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded."},
            headers={
                "Retry-After": str(retry_after),
                "X-RateLimit-Limit": str(ASGI_LIMIT),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(result["reset_in_ms"]),
            },
        )

    response = await call_next(request)
    response.headers["X-RateLimit-Limit"] = str(ASGI_LIMIT)
    response.headers["X-RateLimit-Remaining"] = str(result["remaining"])
    response.headers["X-RateLimit-Reset"] = str(result["reset_in_ms"])
    return response


# ---------------------------------------------------------------------------
# HTTP client helper
# ---------------------------------------------------------------------------


def _request(path: str = "/") -> tuple[int, dict[str, str], str]:
    """Send a synchronous GET request and return (status, headers, body)."""
    conn = http.client.HTTPConnection(HOST, PORT)
    conn.request("GET", path)
    resp = conn.getresponse()
    status = resp.status
    headers = {k.lower(): v for k, v in resp.getheaders()}
    body = resp.read().decode()
    conn.close()
    return status, headers, body


# ---------------------------------------------------------------------------
# Demo sequence
# ---------------------------------------------------------------------------


async def run_demo() -> None:
    """Fire HTTP requests through the rate limiting middleware and print results."""
    print(f"\n{'=' * 50}")
    print("  ASGI Rate Limiting Middleware Demo")
    print(f"  limit={ASGI_LIMIT} requests per {ASGI_WINDOW:.0f}s window")
    print(f"  server: http://{HOST}:{PORT}")
    print(f"{'=' * 50}")

    # Phase 1: Health endpoint bypass
    print("\n--- Phase 1: Health endpoint bypass ---")
    status, headers, _ = await asyncio.to_thread(_request, "/health")
    has_rl = "x-ratelimit-limit" in headers
    print(f"  GET /health -> {status}  rate-limit headers: {'yes' if has_rl else 'no'}")
    logger.info("Phase 1: health=%d, has_rate_limit_headers=%s", status, has_rl)

    # Phase 2: Normal requests (within the limit)
    print(f"\n--- Phase 2: {ASGI_LIMIT} requests within the limit ---")
    for i in range(1, ASGI_LIMIT + 1):
        status, headers, _ = await asyncio.to_thread(_request, "/")
        remaining = headers.get("x-ratelimit-remaining", "?")
        print(f"  Request {i:>3}: {status}  remaining={remaining}")
        logger.info(
            "Phase 2: request=%d, status=%d, remaining=%s", i, status, remaining
        )

    # Phase 3: Exceed the limit
    print(f"\n--- Phase 3: {EXTRA_REQUESTS} requests exceeding the limit ---")
    for i in range(EXTRA_REQUESTS):
        status, headers, _ = await asyncio.to_thread(_request, "/")
        retry_after = headers.get("retry-after", "n/a")
        seq = ASGI_LIMIT + i + 1
        print(f"  Request {seq:>3}: {status}  retry-after={retry_after}s")
        logger.info(
            "Phase 3: request=%d, status=%d, retry_after=%s", seq, status, retry_after
        )

    # Phase 4: Gradual recovery during the next window
    reset_ms = int(headers.get("x-ratelimit-reset", str(int(ASGI_WINDOW * 1000))))
    wait_secs = reset_ms / 1000 + 0.5
    print(
        f"\n--- Phase 4: Gradual recovery (waiting {wait_secs:.1f}s for next window) ---"
    )
    logger.info("Phase 4: sleeping %.1fs for window rollover.", wait_secs)
    await asyncio.sleep(wait_secs)

    recovery_allowed = 0
    recovery_denied = 0
    recovery_total = ASGI_LIMIT
    interval = 0.25
    for i in range(1, recovery_total + 1):
        status, headers, _ = await asyncio.to_thread(_request, "/")
        remaining = headers.get("x-ratelimit-remaining", "?")
        tag = "allowed" if status == 200 else "limited"
        print(f"  Request {i:>3}: {status}  remaining={remaining}  ({tag})")
        logger.info(
            "Phase 4: request=%d, status=%d, remaining=%s", i, status, remaining
        )
        if status == 200:
            recovery_allowed += 1
        else:
            recovery_denied += 1
        if i < recovery_total:
            await asyncio.sleep(interval)

    print(
        f"  Recovery: {recovery_allowed} allowed, {recovery_denied} limited"
        f" (sliding window still decaying)"
    )

    # Phase 5: Health still accessible after exhaustion
    print("\n--- Phase 5: Health endpoint still accessible ---")
    status, headers, _ = await asyncio.to_thread(_request, "/health")
    print(f"  GET /health -> {status}")
    logger.info("Phase 5: health=%d", status)

    # Summary
    total_requests = ASGI_LIMIT + EXTRA_REQUESTS + recovery_total
    print(f"\n{'=' * 50}")
    print(f"  Done. Sent {total_requests} requests to /")
    print(f"  Phase 2: {ASGI_LIMIT} allowed (within limit)")
    print(f"  Phase 3: {EXTRA_REQUESTS} rate-limited (429)")
    print(
        f"  Phase 4: {recovery_allowed} allowed, {recovery_denied} limited (recovery)"
    )
    print("  /health bypassed rate limiting throughout")
    print(f"{'=' * 50}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    setup_logging()

    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning")
    server = uvicorn.Server(config)

    server_task = asyncio.create_task(server.serve())

    # Wait for the server to finish starting.
    while not server.started:
        await asyncio.sleep(0.05)

    try:
        await run_demo()
    finally:
        server.should_exit = True
        await server_task

    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
