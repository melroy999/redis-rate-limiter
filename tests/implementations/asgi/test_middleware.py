"""Tests for the ASGI rate limiting middleware.

Fixture dependencies:
    - ``limiter``, ``_reset_asgi_limiter_class_state``:
      from ``tests/implementations/asgi/conftest.py``.

Direct static-method tests for ``_send_blocked`` and
``_send_error`` use ``AsyncMock`` as the ASGI ``send`` callable
and do not require fixtures.
"""

import inspect
import logging
from unittest.mock import AsyncMock

import pytest

from redis_rate_limiter.backends.asgi import RateLimitMiddleware, by_client_ip
from tests.helpers.utils import assert_log_emitted

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


async def _capture_response(middleware, scope):
    """Run the middleware and capture the response start and body messages."""
    messages = []

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        messages.append(message)

    await middleware(scope, receive, send)
    return messages


@pytest.mark.behavior
class TestRateLimitMiddleware:
    """Tests for the ``RateLimitMiddleware`` ASGI wrapper."""

    @staticmethod
    async def test_allowed_response_includes_rate_limit_headers(limiter):
        """Verify that rate limit headers are injected into allowed responses."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        # Act
        messages = await _capture_response(middleware, _make_scope())
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

    @staticmethod
    async def test_blocked_request_returns_429(limiter):
        """Verify that a blocked request receives a 429 response."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        # Exhaust the rate limit (limit=10).
        scope = _make_scope(client_ip="10.0.0.1")
        for _ in range(10):
            await _capture_response(middleware, scope)

        # Act
        # The next request should be blocked.
        messages = await _capture_response(middleware, scope)

        # Assert
        assert messages[0]["status"] == 429, "blocked request should receive 429"

    @staticmethod
    async def test_blocked_response_includes_retry_after(limiter):
        """Verify that the 429 response includes a ``Retry-After`` header."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        scope = _make_scope(client_ip="10.0.0.2")
        for _ in range(10):
            await _capture_response(middleware, scope)

        # Act
        messages = await _capture_response(middleware, scope)
        header_names = [h[0] for h in messages[0]["headers"]]

        # Assert
        assert b"retry-after" in header_names, (
            "429 response should include Retry-After header"
        )

    @staticmethod
    async def test_custom_on_blocked_callback(limiter):
        """Verify that ``on_blocked`` callback is invoked
        instead of the default 429."""
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
            limiter=limiter,
            key_func=by_client_ip,
            on_blocked=custom_blocked,
        )

        scope = _make_scope(client_ip="10.0.0.3")
        for _ in range(10):
            await _capture_response(middleware, scope)

        # Act
        messages = await _capture_response(middleware, scope)

        # Assert
        assert callback_invoked is True, "custom on_blocked callback should be invoked"
        assert messages[0]["status"] == 503, "custom callback should set status 503"

    @staticmethod
    async def test_non_http_scope_passes_through(limiter):
        """Verify that non-HTTP scopes pass through without rate limiting."""
        # Arrange
        app_invoked = False
        forwarded_scope = None
        forwarded_receive = None
        forwarded_send = None

        async def inner_app(scope, receive, send):
            nonlocal app_invoked, forwarded_scope, forwarded_receive, forwarded_send
            app_invoked = True
            forwarded_scope = scope
            forwarded_receive = receive
            forwarded_send = send

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        scope = _make_scope(scope_type="websocket")

        async def receive():
            return {}

        async def send(message):
            pass

        # Act
        await middleware(scope, receive, send)

        # Assert
        assert app_invoked is True, "non-HTTP scope should pass through to inner app"
        assert forwarded_scope is scope, (
            "non-HTTP scope should forward the original scope to inner app"
        )
        assert forwarded_receive is receive, (
            "non-HTTP scope should forward the original receive callable to inner app"
        )
        assert forwarded_send is send, (
            "non-HTTP scope should forward the original send callable to inner app"
        )

    @staticmethod
    async def test_key_func_none_bypasses_rate_limiting(limiter):
        """Verify that returning ``None`` from ``key_func``
        bypasses rate limiting."""

        # Arrange
        forwarded_scope = None
        forwarded_receive = None

        async def inner_app(scope, receive, send):
            nonlocal forwarded_scope, forwarded_receive
            forwarded_scope = scope
            forwarded_receive = receive
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        def null_key_func(scope):
            return None

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=null_key_func
        )

        original_scope = _make_scope()
        messages = []

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            messages.append(message)

        # Act
        await middleware(original_scope, receive, send)

        # Assert
        assert messages[0]["status"] == 200, "None key should bypass rate limiting"
        header_names = [h[0] for h in messages[0].get("headers", [])]
        assert b"x-ratelimit-limit" not in header_names, (
            "bypassed request should not include rate limit headers"
        )
        assert forwarded_scope is original_scope, (
            "bypassed request should forward the original scope to inner app"
        )
        assert forwarded_receive is receive, (
            "bypassed request should forward the original receive callable to inner app"
        )

    @staticmethod
    async def test_fail_open_allows_on_error(limiter):
        """Verify that ``fail_open`` mode allows the request
        when ``acquire`` raises."""
        # Arrange
        app_invoked = False
        forwarded_receive = None

        async def inner_app(scope, receive, send):
            nonlocal app_invoked, forwarded_receive
            app_invoked = True
            forwarded_receive = receive
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app,
            limiter=limiter,
            key_func=by_client_ip,
            on_error="fail_open",
        )

        limiter.acquire = AsyncMock(side_effect=ConnectionError("redis down"))

        scope = _make_scope()

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            pass

        # Act
        await middleware(scope, receive, send)

        # Assert
        assert app_invoked is True, "fail_open should pass request through on error"
        assert forwarded_receive is receive, (
            "fail_open should forward the original receive callable to inner app"
        )

    @staticmethod
    async def test_fail_closed_returns_503_on_error(limiter):
        """Verify that ``fail_closed`` mode returns 503
        when ``acquire`` raises."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app,
            limiter=limiter,
            key_func=by_client_ip,
            on_error="fail_closed",
        )

        limiter.acquire = AsyncMock(side_effect=ConnectionError("redis down"))

        # Act
        messages = await _capture_response(middleware, _make_scope())

        # Assert
        assert messages[0]["status"] == 503, "fail_closed should return 503 on error"

    @staticmethod
    async def test_wrap_send_preserves_existing_headers(limiter):
        """Verify that ``_wrap_send`` preserves original response
        headers from the inner app."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"x-custom", b"preserved")],
                }
            )
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        # Act
        messages = await _capture_response(middleware, _make_scope())

        # Assert
        header_dict = dict(messages[0]["headers"])
        assert header_dict.get(b"x-custom") == b"preserved", (
            "original headers from the inner app should be preserved"
        )

    @staticmethod
    async def test_wrap_send_header_values_reflect_acquire_result(limiter):
        """Verify that rate limit header values match the
        ``acquire()`` result."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        # Act
        messages = await _capture_response(middleware, _make_scope())

        # Assert
        header_dict = dict(messages[0]["headers"])
        remaining = header_dict.get(b"x-ratelimit-remaining")
        reset = header_dict.get(b"x-ratelimit-reset")
        limit = header_dict.get(b"x-ratelimit-limit")

        assert limit == str(limiter.limit).encode(), (
            "x-ratelimit-limit should match the configured limit"
        )
        assert remaining is not None and remaining != b"None", (
            "x-ratelimit-remaining should be a numeric value, not None"
        )
        assert reset is not None and reset != b"None", (
            "x-ratelimit-reset should be a numeric value, not None"
        )
        assert int(remaining) == limiter.limit - 1, (
            "x-ratelimit-remaining should be limit minus one after first acquire"
        )
        assert int(reset) > 0, "x-ratelimit-reset should be a positive integer"

    @staticmethod
    async def test_inner_app_receives_working_receive_callable(limiter):
        """Verify that the inner app receives the original
        ``receive`` callable, not ``None``."""
        # Arrange
        received_body = None

        async def inner_app(scope, receive, send):
            nonlocal received_body
            request = await receive()
            received_body = request.get("body")
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        # Act
        await _capture_response(middleware, _make_scope())

        # Assert
        assert received_body == b"", (
            "inner app should receive the original receive callable that returns request data"
        )

    @staticmethod
    async def test_wrap_send_handles_missing_headers_key(limiter):
        """Verify that ``_wrap_send`` injects headers even when
        the response lacks a ``headers`` key."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app, limiter=limiter, key_func=by_client_ip
        )

        # Act
        messages = await _capture_response(middleware, _make_scope())
        header_names = [h[0] for h in messages[0]["headers"]]

        # Assert
        assert b"x-ratelimit-limit" in header_names, (
            "rate limit headers should be injected even when inner app omits headers key"
        )


@pytest.mark.behavior
class TestSendBlockedResponse:
    """Tests for ``RateLimitMiddleware._send_blocked``
    response structure and arithmetic."""

    @staticmethod
    @pytest.mark.parametrize(
        ("reset_ms", "expected_retry_after"),
        [(1001, 2), (1000, 1)],
        ids=["1001ms_rounds_up_to_2s", "1000ms_boundary_stays_at_1s"],
    )
    async def test_send_blocked_retry_after_ceiling_division(
        reset_ms, expected_retry_after
    ):
        """Verify that ``_send_blocked`` computes
        ``Retry-After`` via ceiling division."""
        # Arrange
        send = AsyncMock()

        # Act
        await RateLimitMiddleware._send_blocked(send, {"reset_in_ms": reset_ms})

        # Assert
        start_message = send.call_args_list[0].args[0]
        header_dict = dict(start_message["headers"])
        assert header_dict[b"retry-after"] == str(expected_retry_after).encode(), (
            f"retry-after should be {expected_retry_after} for reset_ms={reset_ms}"
        )

    @staticmethod
    async def test_send_blocked_response_structure():
        """Verify that ``_send_blocked`` produces a well-formed
        429 response with correct headers."""
        # Arrange
        send = AsyncMock()
        reset_ms = 2000

        # Act
        await RateLimitMiddleware._send_blocked(send, {"reset_in_ms": reset_ms})

        # Assert
        assert send.call_count == 2, "should send exactly two ASGI messages"

        start_msg = send.call_args_list[0].args[0]
        assert start_msg["type"] == "http.response.start", (
            "first message type should be http.response.start"
        )
        assert start_msg["status"] == 429, "status should be 429"
        header_dict = dict(start_msg["headers"])
        assert header_dict[b"content-type"] == b"text/plain; charset=utf-8", (
            "content-type should be text/plain; charset=utf-8"
        )
        assert header_dict[b"x-ratelimit-remaining"] == b"0", (
            "x-ratelimit-remaining should be 0"
        )
        assert header_dict[b"x-ratelimit-reset"] == b"2000", (
            "x-ratelimit-reset should equal the reset_ms value"
        )

        body_msg = send.call_args_list[1].args[0]
        assert body_msg["type"] == "http.response.body", (
            "second message type should be http.response.body"
        )
        assert body_msg["body"] == b"Rate limit exceeded. Please retry later.", (
            "response body should be the default blocked message"
        )


@pytest.mark.behavior
class TestSendErrorResponse:
    """Tests for ``RateLimitMiddleware._send_error``
    response structure."""

    @staticmethod
    async def test_send_error_response_structure():
        """Verify that ``_send_error`` produces a well-formed
        503 response."""
        # Arrange
        send = AsyncMock()

        # Act
        await RateLimitMiddleware._send_error(send)

        # Assert
        assert send.call_count == 2, "should send exactly two ASGI messages"

        start_msg = send.call_args_list[0].args[0]
        assert start_msg["type"] == "http.response.start", (
            "first message type should be http.response.start"
        )
        assert start_msg["status"] == 503, "status should be 503"
        assert start_msg["headers"] == [
            (b"content-type", b"text/plain; charset=utf-8")
        ], "headers should contain only the content-type header"

        body_msg = send.call_args_list[1].args[0]
        assert isinstance(body_msg, dict), (
            "second send argument must be a dict, not None"
        )
        assert body_msg["type"] == "http.response.body", (
            "second message type should be http.response.body"
        )
        assert body_msg["body"] == b"Service temporarily unavailable.", (
            "response body should be the default error message"
        )


# ---------------------------------------------------------------------------
# Observability tests
# ---------------------------------------------------------------------------


@pytest.mark.observability
class TestMiddlewareObservability:
    """Observability tests for the ``RateLimitMiddleware`` log emissions."""

    @staticmethod
    async def test_acquire_error_emits_exception_log(limiter, caplog):
        """Verify that the middleware emits an ERROR log with
        limiter id and key when ``acquire`` raises."""

        # Arrange
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"OK"})

        middleware = RateLimitMiddleware(
            inner_app,
            limiter=limiter,
            key_func=by_client_ip,
            on_error="fail_open",
        )

        limiter.acquire = AsyncMock(side_effect=ConnectionError("redis down"))

        # Act
        with caplog.at_level(
            logging.ERROR, logger="redis_rate_limiter.backends.asgi.middleware"
        ):
            await _capture_response(middleware, _make_scope())

        # Assert
        assert_log_emitted(
            caplog.records,
            level="ERROR",
            required_fragments=[
                f"limiter={limiter.id}",
                "key=127.0.0.1",
            ],
            message="should emit an error log containing the limiter id and key on acquire failure",
        )


# ---------------------------------------------------------------------------
# Signature tests
# ---------------------------------------------------------------------------


@pytest.mark.signature
class TestMiddlewareSignatures:
    """Signature tests for ``RateLimitMiddleware`` default parameter values."""

    @staticmethod
    def test_middleware_init_default_parameters():
        """Verify that ``on_error`` and ``on_blocked`` have
        the expected defaults.

        Mutation target: ``on_error`` and ``on_blocked`` default
        values in ``RateLimitMiddleware.__init__``.
        """
        # Arrange & Act
        sig = inspect.signature(RateLimitMiddleware.__init__)

        # Assert
        assert sig.parameters["on_error"].default == "fail_open", (
            "default on_error must be lowercase 'fail_open'"
        )
        assert sig.parameters["on_blocked"].default is None, (
            "default on_blocked must be None so the built-in 429 handler is used"
        )
