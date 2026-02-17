"""Unit tests for the Prometheus metrics exporter integration."""

import pytest
from prometheus_client import CollectorRegistry

from celery_rate_limiter.integrations.prometheus import PrometheusMetricsExporter


@pytest.fixture
def registry():
    """Provide a fresh Prometheus registry to avoid cross-test pollution."""
    return CollectorRegistry()


@pytest.fixture
def exporter(registry, default_limiter_id):
    """Create a PrometheusMetricsExporter backed by an isolated registry."""
    return PrometheusMetricsExporter(limiter_id=default_limiter_id, registry=registry)


# ---------------------------------------------------------------------------
# Consume event: counter increments
# ---------------------------------------------------------------------------


class TestConsumeCounters:
    """Verify that consume events increment the correct outcome counter."""

    def test_success_increments_counter(self, exporter, registry, default_limiter_id):
        """Verify that a successful consume event increments the success counter."""
        # Act
        exporter(
            "consume",
            {
                "success": True,
                "expired": False,
                "remaining_tokens": 10,
                "active_concurrency": 2,
                "reset_in_ms": 500,
                "remaining_tasks": 5,
            },
        )

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_consume_total",
            {"limiter_id": default_limiter_id, "outcome": "success"},
        )
        assert value == 1.0, "success counter should be incremented to 1"

    def test_rejected_increments_counter(self, exporter, registry, default_limiter_id):
        """Verify that a rejected consume event increments the rejected counter."""
        # Act
        exporter(
            "consume",
            {
                "success": False,
                "expired": False,
                "remaining_tokens": 0,
                "active_concurrency": 3,
                "reset_in_ms": 200,
                "remaining_tasks": 8,
            },
        )

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_consume_total",
            {"limiter_id": default_limiter_id, "outcome": "rejected"},
        )
        assert value == 1.0, "rejected counter should be incremented to 1"

    def test_expired_increments_counter(self, exporter, registry, default_limiter_id):
        """Verify that an expired consume event increments the expired counter."""
        # Act
        exporter(
            "consume",
            {
                "success": False,
                "expired": True,
                "remaining_tokens": 5,
                "active_concurrency": 1,
                "reset_in_ms": 300,
                "remaining_tasks": 0,
            },
        )

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_consume_total",
            {"limiter_id": default_limiter_id, "outcome": "expired"},
        )
        assert value == 1.0, "expired counter should be incremented to 1"

    def test_multiple_events_accumulate(self, exporter, registry, default_limiter_id):
        """Verify that multiple consume events accumulate in the counter."""
        # Act
        for _ in range(5):
            exporter(
                "consume",
                {
                    "success": True,
                    "expired": False,
                    "remaining_tokens": 10,
                    "active_concurrency": 1,
                    "reset_in_ms": 500,
                    "remaining_tasks": 3,
                },
            )

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_consume_total",
            {"limiter_id": default_limiter_id, "outcome": "success"},
        )
        assert value == 5.0, "success counter should accumulate to 5"


# ---------------------------------------------------------------------------
# Consume event: gauge updates
# ---------------------------------------------------------------------------


class TestConsumeGauges:
    """Verify that consume events update the point-in-time gauges."""

    def test_gauges_updated_on_consume(self, exporter, registry, default_limiter_id):
        """Verify that all gauges are set after a consume event."""
        # Act
        exporter(
            "consume",
            {
                "success": True,
                "expired": False,
                "remaining_tokens": 18,
                "active_concurrency": 3,
                "reset_in_ms": 400,
                "remaining_tasks": 7,
            },
        )

        # Assert
        assert (
            registry.get_sample_value(
                "celery_rate_limiter_remaining_tokens",
                {"limiter_id": default_limiter_id},
            )
            == 18.0
        ), "remaining tokens gauge should reflect the event value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_active_concurrency",
                {"limiter_id": default_limiter_id},
            )
            == 3.0
        ), "active concurrency gauge should reflect the event value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_buffer_depth",
                {"limiter_id": default_limiter_id},
            )
            == 7.0
        ), "buffer depth gauge should reflect the event value"

    def test_gauges_reflect_latest_value(self, exporter, registry, default_limiter_id):
        """Verify that gauges reflect the most recent event, not accumulate."""
        # Arrange
        exporter(
            "consume",
            {
                "success": True,
                "expired": False,
                "remaining_tokens": 20,
                "active_concurrency": 1,
                "reset_in_ms": 500,
                "remaining_tasks": 10,
            },
        )

        # Act
        exporter(
            "consume",
            {
                "success": True,
                "expired": False,
                "remaining_tokens": 5,
                "active_concurrency": 3,
                "reset_in_ms": 200,
                "remaining_tasks": 2,
            },
        )

        # Assert
        assert (
            registry.get_sample_value(
                "celery_rate_limiter_remaining_tokens",
                {"limiter_id": default_limiter_id},
            )
            == 5.0
        ), "remaining tokens gauge should reflect the latest value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_active_concurrency",
                {"limiter_id": default_limiter_id},
            )
            == 3.0
        ), "active concurrency gauge should reflect the latest value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_buffer_depth",
                {"limiter_id": default_limiter_id},
            )
            == 2.0
        ), "buffer depth gauge should reflect the latest value"


# ---------------------------------------------------------------------------
# Schedule event
# ---------------------------------------------------------------------------


class TestScheduleCounter:
    """Verify that schedule events increment the correct counter."""

    def test_scheduled_true(self, exporter, registry, default_limiter_id):
        """Verify that a successful schedule event increments the true counter."""
        # Act
        exporter("schedule", {"scheduled": True, "task_id": "abc123"})

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_schedule_total",
            {"limiter_id": default_limiter_id, "scheduled": "true"},
        )
        assert value == 1.0, "scheduled=true counter should be incremented to 1"

    def test_scheduled_false(self, exporter, registry, default_limiter_id):
        """Verify that a duplicate schedule event increments the false counter."""
        # Act
        exporter("schedule", {"scheduled": False, "task_id": "abc123"})

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_schedule_total",
            {"limiter_id": default_limiter_id, "scheduled": "false"},
        )
        assert value == 1.0, "scheduled=false counter should be incremented to 1"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Verify graceful handling of unexpected inputs."""

    def test_unknown_event_is_ignored(self, exporter, registry):
        """Verify that unknown event names do not raise exceptions."""
        # Act & Assert
        # This invocation must not raise.
        exporter("unknown_event", {"key": "value"})

    def test_custom_registry_isolation(self):
        """Verify that metrics registered on a custom registry do not appear on another."""
        # Arrange
        registry_a = CollectorRegistry()
        registry_b = CollectorRegistry()

        exporter_a = PrometheusMetricsExporter(limiter_id="a", registry=registry_a)
        PrometheusMetricsExporter(limiter_id="b", registry=registry_b)

        # Act
        exporter_a(
            "consume",
            {
                "success": True,
                "expired": False,
                "remaining_tokens": 10,
                "active_concurrency": 1,
                "reset_in_ms": 500,
                "remaining_tasks": 0,
            },
        )

        # Assert
        assert (
            registry_a.get_sample_value(
                "celery_rate_limiter_consume_total",
                {"limiter_id": "a", "outcome": "success"},
            )
            == 1.0
        ), "registry A should contain the metric from exporter A"

        assert (
            registry_b.get_sample_value(
                "celery_rate_limiter_consume_total",
                {"limiter_id": "a", "outcome": "success"},
            )
            is None
        ), "registry B should not contain metrics from exporter A"
