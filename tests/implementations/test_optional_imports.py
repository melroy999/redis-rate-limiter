"""Tests for optional dependency import guards in the package ``__init__``.

Each test simulates a missing optional dependency by patching
``sys.modules`` and reloading the package. This verifies that:

    1. The library does not raise when optional backends are absent.
    2. Backend-specific symbols are excluded from ``__all__``.
    3. Non-optional backends remain fully functional.

Fixture dependencies: none (pure import tests, no Redis required).
"""

import importlib
import sys
from unittest.mock import patch

import pytest


@pytest.mark.behavior
class TestOptionalImportGuards:
    """Verify graceful degradation when optional packages are absent."""

    @staticmethod
    def test_celery_backend_excluded_when_celery_missing():
        """Verify that ``CeleryRateLimiter`` is absent from
        ``__all__`` when celery is not installed and that
        non-optional backends remain accessible.
        """
        # Arrange
        blocked = {
            "celery": None,
            "celery.app": None,
            "celery.result": None,
            "redis_rate_limiter.backends.celery": None,
            "redis_rate_limiter.backends.celery.limiter": None,
        }

        # Act
        with patch.dict(sys.modules, blocked):
            module = importlib.reload(importlib.import_module("redis_rate_limiter"))

            # Assert
            assert "CeleryRateLimiter" not in module.__all__, (
                "CeleryRateLimiter should not be in __all__ when celery is missing"
            )
            assert hasattr(module, "ThreadPoolRateLimiter"), (
                "ThreadPoolRateLimiter should remain accessible"
            )
            assert hasattr(module, "ProcessPoolRateLimiter"), (
                "ProcessPoolRateLimiter should remain accessible"
            )

        # Teardown
        importlib.reload(importlib.import_module("redis_rate_limiter"))

    @staticmethod
    def test_rq_backend_excluded_when_rq_missing():
        """Verify that ``RQRateLimiter`` is absent from
        ``__all__`` when rq is not installed and that
        non-optional backends remain accessible.
        """
        # Arrange
        blocked = {
            "rq": None,
            "rq.job": None,
            "rq.queue": None,
            "redis_rate_limiter.backends.rq": None,
            "redis_rate_limiter.backends.rq.limiter": None,
        }

        # Act
        with patch.dict(sys.modules, blocked):
            module = importlib.reload(importlib.import_module("redis_rate_limiter"))

            # Assert
            assert "RQRateLimiter" not in module.__all__, (
                "RQRateLimiter should not be in __all__ when rq is missing"
            )
            assert hasattr(module, "ASGIRateLimiter"), (
                "ASGIRateLimiter should remain accessible"
            )
            assert hasattr(module, "AsyncIOTaskLimiter"), (
                "AsyncIOTaskLimiter should remain accessible"
            )

        # Teardown
        importlib.reload(importlib.import_module("redis_rate_limiter"))

    @staticmethod
    def test_prometheus_excluded_when_prometheus_client_missing():
        """Verify that ``PrometheusMetricsExporter`` is absent from
        ``__all__`` when prometheus_client is not installed and that
        non-optional backends remain accessible.
        """
        # Arrange
        blocked = {
            "prometheus_client": None,
            "redis_rate_limiter.integrations.prometheus": None,
        }

        # Act
        with patch.dict(sys.modules, blocked):
            module = importlib.reload(importlib.import_module("redis_rate_limiter"))

            # Assert
            assert "PrometheusMetricsExporter" not in module.__all__, (
                "PrometheusMetricsExporter should not be in __all__ "
                "when prometheus_client is missing"
            )
            assert hasattr(module, "ThreadPoolRateLimiter"), (
                "ThreadPoolRateLimiter should remain accessible"
            )

        # Teardown
        importlib.reload(importlib.import_module("redis_rate_limiter"))

    @staticmethod
    def test_core_symbols_available_without_optional_dependencies():
        """Verify that core symbols remain importable when all
        optional dependencies are absent.
        """
        # Arrange
        blocked = {
            "celery": None,
            "celery.app": None,
            "celery.result": None,
            "rq": None,
            "rq.job": None,
            "rq.queue": None,
            "prometheus_client": None,
            "redis_rate_limiter.backends.celery": None,
            "redis_rate_limiter.backends.celery.limiter": None,
            "redis_rate_limiter.backends.rq": None,
            "redis_rate_limiter.backends.rq.limiter": None,
            "redis_rate_limiter.integrations.prometheus": None,
        }

        # Act
        with patch.dict(sys.modules, blocked):
            module = importlib.reload(importlib.import_module("redis_rate_limiter"))

            # Assert
            core_symbols = [
                "AbstractDistributedRateLimiter",
                "AbstractAsyncDistributedRateLimiter",
                "ASGIRateLimiter",
                "AsyncIOTaskLimiter",
                "ProcessPoolRateLimiter",
                "ThreadPoolRateLimiter",
            ]
            for symbol in core_symbols:
                assert symbol in module.__all__, (
                    f"{symbol} should remain in __all__ without optional dependencies"
                )
                assert hasattr(module, symbol), (
                    f"{symbol} should be an accessible attribute"
                )

        # Teardown
        importlib.reload(importlib.import_module("redis_rate_limiter"))


@pytest.mark.behavior
class TestPrometheusImportGuard:
    """Verify the prometheus integration import guard."""

    @staticmethod
    def test_prometheus_module_raises_helpful_error_when_missing():
        """Verify that importing the prometheus module directly
        raises an ``ImportError`` with installation instructions.
        """
        # Arrange
        blocked = {"prometheus_client": None}

        # Act & Assert
        with patch.dict(sys.modules, blocked):
            sys.modules.pop("redis_rate_limiter.integrations.prometheus", None)
            with pytest.raises(ImportError, match="prometheus_client"):
                importlib.import_module("redis_rate_limiter.integrations.prometheus")

        # Teardown
        sys.modules.pop("redis_rate_limiter.integrations.prometheus", None)
        importlib.import_module("redis_rate_limiter.integrations.prometheus")
