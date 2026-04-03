"""Tests for the ASGI key extraction functions."""

import pytest

from redis_rate_limiter.backends.asgi.keys import by_client_ip, by_header


@pytest.mark.behavior
class TestByClientIp:
    """Tests for the ``by_client_ip`` key function."""

    @staticmethod
    def test_extracts_ip_from_client_tuple():
        """Verify that the client IP is extracted from the scope."""
        # Arrange
        scope = {"client": ("192.168.1.1", 8080)}

        # Act
        result = by_client_ip(scope)

        # Assert
        assert result == "192.168.1.1", "client IP should be extracted from scope"

    @staticmethod
    def test_returns_none_when_client_missing():
        """Verify that ``None`` is returned when the client field is absent."""
        # Arrange
        scope = {}

        # Act
        result = by_client_ip(scope)

        # Assert
        assert result is None, "missing client field should yield None"

    @staticmethod
    def test_returns_none_when_client_is_none():
        """Verify that ``None`` is returned when the client field is ``None``."""
        # Arrange
        scope = {"client": None}

        # Act
        result = by_client_ip(scope)

        # Assert
        assert result is None, "None client field should yield None"

    @staticmethod
    def test_returns_none_when_client_is_empty():
        """Verify that ``None`` is returned when the client tuple is empty."""
        # Arrange
        scope = {"client": ()}

        # Act
        result = by_client_ip(scope)

        # Assert
        assert result is None, "empty client tuple should yield None"


@pytest.mark.behavior
class TestByHeader:
    """Tests for the ``by_header`` key function factory."""

    @staticmethod
    def test_extracts_header_value():
        """Verify that the correct header value is extracted."""
        # Arrange
        key_func = by_header("x-api-key")
        scope = {
            "headers": [
                (b"content-type", b"application/json"),
                (b"x-api-key", b"secret-123"),
            ]
        }

        # Act
        result = key_func(scope)

        # Assert
        assert result == "secret-123", "header value should be extracted from scope"

    @staticmethod
    def test_case_insensitive_matching():
        """Verify that header matching is case-insensitive."""
        # Arrange
        key_func = by_header("X-API-Key")
        scope = {
            "headers": [
                (b"x-api-key", b"secret-456"),
            ]
        }

        # Act
        result = key_func(scope)

        # Assert
        assert result == "secret-456", (
            "header name comparison should be case-insensitive"
        )

    @staticmethod
    def test_returns_none_when_header_absent():
        """Verify that ``None`` is returned when the header is not present."""
        # Arrange
        key_func = by_header("x-api-key")
        scope = {
            "headers": [
                (b"content-type", b"application/json"),
            ]
        }

        # Act
        result = key_func(scope)

        # Assert
        assert result is None, "absent header should yield None"

    @staticmethod
    def test_returns_none_when_no_headers():
        """Verify that ``None`` is returned when the headers field is absent."""
        # Arrange
        key_func = by_header("x-api-key")
        scope = {}

        # Act
        result = key_func(scope)

        # Assert
        assert result is None, "missing headers field should yield None"
