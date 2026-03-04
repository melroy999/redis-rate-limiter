"""Key extraction functions for ASGI rate limiting.

Each function maps an ASGI scope to a rate limit identity string. Returning
``None`` signals that the request should pass through without rate limiting.
"""

from __future__ import annotations

from typing import Callable, Optional

from redis_rate_limiter.backends.asgi.types import Scope


def by_client_ip(scope: Scope) -> Optional[str]:
    """Extract the client IP address from the ASGI scope.

    Returns ``None`` when the ``client`` field is absent or empty.
    """
    client = scope.get("client")
    if client:
        return str(client[0])
    return None


def by_header(name: str) -> Callable[[Scope], Optional[str]]:
    """Return a key function that extracts a rate limit identity from an HTTP header.

    The header name is case-insensitive and pre-encoded for efficient comparison
    against the raw ASGI headers list.

    Args:
        name: The HTTP header name (e.g., ``"x-api-key"``).

    Returns:
        A callable that extracts the header value from an ASGI scope.
    """
    # ASGI headers are raw byte pairs; Latin-1 (ISO-8859-1) is the standard
    # HTTP/1.1 header encoding and provides a lossless byte-to-character mapping.
    target = name.lower().encode("latin-1")  # pragma: no mutate

    def _extract(scope: Scope) -> Optional[str]:
        for header_name, header_value in scope.get("headers", []):
            if header_name == target:
                return str(header_value.decode("latin-1"))  # pragma: no mutate
        return None

    return _extract
