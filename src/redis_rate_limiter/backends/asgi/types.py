"""ASGI protocol types and rate limiter result structures."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, MutableMapping, Optional, TypedDict

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

# Key extraction function type: extracts a rate limit identity from the ASGI
# scope. Returning ``None`` signals that the request should pass through
# without rate limiting (e.g., a health check or an unmatched tier).
KeyFunc = Callable[[Scope], Optional[str]]


class AcquireResult(TypedDict):
    """Structured representation of the result returned by the acquire Lua script."""

    allowed: bool
    remaining: int
    reset_in_ms: int
    val_previous: int
    val_current: int
