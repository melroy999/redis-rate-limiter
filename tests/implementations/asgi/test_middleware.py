"""Tests for the ASGI rate limiting middleware."""

from unittest.mock import AsyncMock

from celery_rate_limiter.backends.asgi import RateLimitMiddleware, by_client_ip


class TestRateLimitMiddleware:
    """Tests for the ``RateLimitMiddleware`` ASGI wrapper."""

    @staticmethod
    def _make_scope(
        client_ip: str = "127.0.0.1",
        scope_type: str = "http",
    ) -> dict:
        """Create a minimal ASGI HTTP scope."""
        return {
            "type": scope_type,
            "method": "GET",
            "path": "/",
            "headers": [],
            "client": (client_ip, 12345),
        }

    @staticmethod
    async def _capture_response(middleware, scope):
        """Run the middleware and capture the response start and body messages."""
        messages = []

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            messages.append(message)

        await middleware(scope, receive, send)
        return messages

    async def test_allowed_request_passes_through(self, asgi_limiter):
        """Verify that an allowed request receives 200 from the inner app."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=asgi_limiter, key_func=by_client_ip
        )

        # Act
        messages = await self._capture_response(middleware, self._make_scope())

        # Assert
        assert messages[0]["status"] == 200, "allowed request should receive 200"

    async def test_allowed_response_includes_rate_limit_headers(self, asgi_limiter):
        """Verify that rate limit headers are injected into allowed responses."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=asgi_limiter, key_func=by_client_ip
        )

        # Act
        messages = await self._capture_response(middleware, self._make_scope())
        header_names = [h[0] for h in messages[0]["headers"]]

        # Assert
        assert b"x-ratelimit-limit" in header_names, (
            "response should include x-ratelimit-limit header"
        )
        assert b"x-ratelimit-remaining" in header_names, (
            "response should include x-ratelimit-remaining header"
        )
        assert b"x-ratelimit-reset" in header_names, (
            "response should include x-ratelimit-reset header"
        )

    async def test_blocked_request_returns_429(self, asgi_limiter):
        """Verify that a blocked request receives a 429 response."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=asgi_limiter, key_func=by_client_ip
        )

        # Exhaust the rate limit (limit=10).
        scope = self._make_scope(client_ip="10.0.0.1")
        for _ in range(10):
            await self._capture_response(middleware, scope)

        # Act
        # The next request should be blocked.
        messages = await self._capture_response(middleware, scope)

        # Assert
        assert messages[0]["status"] == 429, "blocked request should receive 429"

    async def test_blocked_response_includes_retry_after(self, asgi_limiter):
        """Verify that the 429 response includes a ``Retry-After`` header."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=asgi_limiter, key_func=by_client_ip
        )

        scope = self._make_scope(client_ip="10.0.0.2")
        for _ in range(10):
            await self._capture_response(middleware, scope)

        # Act
        messages = await self._capture_response(middleware, scope)
        header_names = [h[0] for h in messages[0]["headers"]]

        # Assert
        assert b"retry-after" in header_names, (
            "429 response should include Retry-After header"
        )

    async def test_custom_on_blocked_callback(self, asgi_limiter):
        """Verify that ``on_blocked`` callback is invoked instead of the default 429."""
        # Arrange
        callback_invoked = False

        async def custom_blocked(scope, result, send):
            nonlocal callback_invoked
            callback_invoked = True
            await send({"type": "http.response.start", "status": 503, "headers": []})
            await send({"type": "http.response.body", "body": b"Custom blocked"})

        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app,
            limiter=asgi_limiter,
            key_func=by_client_ip,
            on_blocked=custom_blocked,
        )

        scope = self._make_scope(client_ip="10.0.0.3")
        for _ in range(10):
            await self._capture_response(middleware, scope)

        # Act
        messages = await self._capture_response(middleware, scope)

        # Assert
        assert callback_invoked is True, "custom on_blocked callback should be invoked"
        assert messages[0]["status"] == 503, "custom callback should set status 503"

    async def test_non_http_scope_passes_through(self, asgi_limiter):
        """Verify that non-HTTP scopes pass through without rate limiting."""
        # Arrange
        app_invoked = False

        async def inner_app(scope, receive, send):
            nonlocal app_invoked
            app_invoked = True

        middleware = RateLimitMiddleware(
            inner_app, limiter=asgi_limiter, key_func=by_client_ip
        )

        scope = self._make_scope(scope_type="websocket")
        messages = []

        async def receive():
            return {}

        async def send(message):
            messages.append(message)

        # Act
        await middleware(scope, receive, send)

        # Assert
        assert app_invoked is True, "non-HTTP scope should pass through to inner app"

    async def test_key_func_none_bypasses_rate_limiting(self, asgi_limiter):
        """Verify that returning ``None`` from ``key_func`` bypasses rate limiting."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        def null_key_func(scope):
            return None

        middleware = RateLimitMiddleware(
            inner_app, limiter=asgi_limiter, key_func=null_key_func
        )

        # Act
        messages = await self._capture_response(middleware, self._make_scope())

        # Assert
        assert messages[0]["status"] == 200, "None key should bypass rate limiting"
        # No rate limit headers should be present since rate limiting was bypassed.
        header_names = [h[0] for h in messages[0].get("headers", [])]
        assert b"x-ratelimit-limit" not in header_names, (
            "bypassed request should not include rate limit headers"
        )

    async def test_fail_open_allows_on_error(self, asgi_limiter):
        """Verify that ``fail_open`` mode allows the request when ``acquire`` raises."""
        # Arrange
        app_invoked = False

        async def inner_app(scope, receive, send):
            nonlocal app_invoked
            app_invoked = True
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app,
            limiter=asgi_limiter,
            key_func=by_client_ip,
            on_error="fail_open",
        )

        asgi_limiter.acquire = AsyncMock(side_effect=ConnectionError("redis down"))

        # Act
        await self._capture_response(middleware, self._make_scope())

        # Assert
        assert app_invoked is True, "fail_open should pass request through on error"

    async def test_fail_closed_returns_503_on_error(self, asgi_limiter):
        """Verify that ``fail_closed`` mode returns 503 when ``acquire`` raises."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app,
            limiter=asgi_limiter,
            key_func=by_client_ip,
            on_error="fail_closed",
        )

        asgi_limiter.acquire = AsyncMock(side_effect=ConnectionError("redis down"))

        # Act
        messages = await self._capture_response(middleware, self._make_scope())

        # Assert
        assert messages[0]["status"] == 503, "fail_closed should return 503 on error"
