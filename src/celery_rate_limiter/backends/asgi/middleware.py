"""ASGI rate limiting middleware.

Framework-agnostic ASGI middleware that wraps any ASGI application with
per-identity rate limiting. Non-HTTP scopes and requests where the key
function returns ``None`` pass through without rate limiting.
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable, Literal, Optional

from celery_rate_limiter.backends.asgi.limiter import ASGIRateLimiter
from celery_rate_limiter.backends.asgi.types import (
    AcquireResult,
    ASGIApp,
    KeyFunc,
    Receive,
    Scope,
    Send,
)

logger = logging.getLogger(__name__)


class RateLimitMiddleware:
    """ASGI middleware that applies per-identity rate limiting to HTTP requests.

    Non-HTTP scopes (e.g., WebSocket, lifespan) pass through unconditionally.
    When ``key_func`` returns ``None`` for a request, the request also passes
    through without rate limiting.

    Rate limit response headers (``X-RateLimit-Limit``, ``X-RateLimit-Remaining``,
    ``X-RateLimit-Reset``) are injected into all HTTP responses when a key is
    resolved.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        limiter: ASGIRateLimiter,
        key_func: KeyFunc,
        on_blocked: Optional[
            Callable[[Scope, AcquireResult, Send], Awaitable[None]]
        ] = None,
        on_error: Literal["fail_open", "fail_closed"] = "fail_open",
    ) -> None:
        """Initialize the rate limiting middleware.

        Args:
            app: The inner ASGI application.
            limiter: The ``ASGIRateLimiter`` instance used for rate limit checks.
            key_func: A callable that extracts a rate limit identity from the ASGI
                scope. Returning ``None`` bypasses rate limiting for the request.
            on_blocked: An optional async callback invoked when a request is blocked.
                When provided, the default 429 response is skipped entirely. The
                callback receives the scope, the acquire result, and the ASGI send
                callable.
            on_error: The behavior when an error occurs during rate limit evaluation.
                ``"fail_open"`` allows the request to proceed; ``"fail_closed"``
                returns a 503 Service Unavailable response.
        """
        self.app = app
        self.limiter = limiter
        self.key_func = key_func
        self.on_blocked = on_blocked
        self.on_error = on_error

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Process an ASGI request through the rate limiting pipeline."""
        # Non-HTTP scopes pass through unconditionally.
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        # Extract the rate limit identity from the scope.
        key = self.key_func(scope)
        if key is None:
            await self.app(scope, receive, send)
            return

        # Attempt to acquire a rate limit token.
        try:
            result = await self.limiter.acquire(key)
        except Exception:
            logger.exception(
                "Rate limit check failed: limiter=%s, key=%s.",
                self.limiter.id,
                key,
            )
            if self.on_error == "fail_closed":
                await self._send_error(send)
                return
            # fail_open: proceed without rate limit headers.
            await self.app(scope, receive, send)
            return

        if not result["allowed"]:
            if self.on_blocked is not None:
                await self.on_blocked(scope, result, send)
            else:
                await self._send_blocked(send, result)
            return

        # Request allowed: inject rate limit headers into the response.
        await self.app(scope, receive, self._wrap_send(send, result))

    def _wrap_send(self, send: Send, result: AcquireResult) -> Send:
        """Return a wrapped send callable that injects rate limit headers."""

        async def _send(message: dict) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    [
                        (b"x-ratelimit-limit", str(self.limiter.limit).encode()),
                        (b"x-ratelimit-remaining", str(result["remaining"]).encode()),
                        (b"x-ratelimit-reset", str(result["reset_in_ms"]).encode()),
                    ]
                )
                message = {**message, "headers": headers}
            await send(message)

        return _send

    @staticmethod
    async def _send_blocked(send: Send, result: AcquireResult) -> None:
        """Send a default plain-text 429 Too Many Requests response."""
        reset_ms = result["reset_in_ms"]
        retry_after = max(1, (reset_ms + 999) // 1000)
        await send(
            {
                "type": "http.response.start",
                "status": 429,
                "headers": [
                    (b"content-type", b"text/plain; charset=utf-8"),
                    (b"retry-after", str(retry_after).encode()),
                    (b"x-ratelimit-remaining", b"0"),
                    (b"x-ratelimit-reset", str(reset_ms).encode()),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"Rate limit exceeded. Please retry later.",
            }
        )

    @staticmethod
    async def _send_error(send: Send) -> None:
        """Send a 503 Service Unavailable response for fail-closed errors."""
        await send(
            {
                "type": "http.response.start",
                "status": 503,
                "headers": [
                    (b"content-type", b"text/plain; charset=utf-8"),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"Service temporarily unavailable.",
            }
        )
