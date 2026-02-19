"""Unit tests for the Prometheus metrics exporter integration."""

import logging

import pytest
from prometheus_client import CollectorRegistry

from celery_rate_limiter.integrations.prometheus import PrometheusMetricsExporter


@pytest.fixture
def registry():
    """Provide a fresh Prometheus registry to avoid cross-test pollution."""
    return CollectorRegistry()


@pytest.fixture
def exporter(registry, limiter_id):
    """Create a PrometheusMetricsExporter backed by an isolated registry."""
    return PrometheusMetricsExporter(limiter_id=limiter_id, registry=registry)


class TestConsumeCounters:
    """Verify that consume events increment the correct outcome counter."""

    @staticmethod
    def test_success_increments_counter(exporter, registry, limiter_id):
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
            {"limiter_id": limiter_id, "outcome": "success"},
        )
        assert value == 1.0, "success counter should be incremented to 1"

    @staticmethod
    def test_rejected_increments_counter(exporter, registry, limiter_id):
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
            {"limiter_id": limiter_id, "outcome": "rejected"},
        )
        assert value == 1.0, "rejected counter should be incremented to 1"

    @staticmethod
    def test_expired_increments_counter(exporter, registry, limiter_id):
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
            {"limiter_id": limiter_id, "outcome": "expired"},
        )
        assert value == 1.0, "expired counter should be incremented to 1"

    @staticmethod
    def test_multiple_events_accumulate(exporter, registry, limiter_id):
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
            {"limiter_id": limiter_id, "outcome": "success"},
        )
        assert value == 5.0, "success counter should accumulate to 5"


class TestConsumeGauges:
    """Verify that consume events update the point-in-time gauges."""

    @staticmethod
    def test_gauges_updated_on_consume(exporter, registry, limiter_id):
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
                {"limiter_id": limiter_id},
            )
            == 18.0
        ), "remaining tokens gauge should reflect the event value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_active_concurrency",
                {"limiter_id": limiter_id},
            )
            == 3.0
        ), "active concurrency gauge should reflect the event value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_buffer_depth",
                {"limiter_id": limiter_id},
            )
            == 7.0
        ), "buffer depth gauge should reflect the event value"

    @staticmethod
    def test_gauges_reflect_latest_value(exporter, registry, limiter_id):
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
                {"limiter_id": limiter_id},
            )
            == 5.0
        ), "remaining tokens gauge should reflect the latest value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_active_concurrency",
                {"limiter_id": limiter_id},
            )
            == 3.0
        ), "active concurrency gauge should reflect the latest value"

        assert (
            registry.get_sample_value(
                "celery_rate_limiter_buffer_depth",
                {"limiter_id": limiter_id},
            )
            == 2.0
        ), "buffer depth gauge should reflect the latest value"


class TestScheduleCounter:
    """Verify that schedule events increment the correct counter."""

    @staticmethod
    def test_scheduled_true(exporter, registry, limiter_id):
        """Verify that a successful schedule event increments the true counter."""
        # Act
        exporter("schedule", {"scheduled": True, "task_id": "abc123"})

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_schedule_total",
            {"limiter_id": limiter_id, "scheduled": "true"},
        )
        assert value == 1.0, "scheduled=true counter should be incremented to 1"

    @staticmethod
    def test_scheduled_false(exporter, registry, limiter_id):
        """Verify that a duplicate schedule event increments the false counter."""
        # Act
        exporter("schedule", {"scheduled": False, "task_id": "abc123"})

        # Assert
        value = registry.get_sample_value(
            "celery_rate_limiter_schedule_total",
            {"limiter_id": limiter_id, "scheduled": "false"},
        )
        assert value == 1.0, "scheduled=false counter should be incremented to 1"


class TestMetricRegistration:
    """Verify that Prometheus metrics are registered with the correct identity."""

    @staticmethod
    def _collect_metrics(registry):
        """Collect all metric families from the registry into a name-keyed dictionary."""
        return {m.name: m for m in registry.collect()}

    @staticmethod
    def test_all_expected_metric_names_are_registered(registry, exporter):
        """Verify that all five expected metric names are present in the registry."""
        # Act
        metric_names = {m.name for m in registry.collect()}

        # Assert
        expected = {
            "celery_rate_limiter_consume",
            "celery_rate_limiter_schedule",
            "celery_rate_limiter_remaining_tokens",
            "celery_rate_limiter_active_concurrency",
            "celery_rate_limiter_buffer_depth",
        }
        assert expected.issubset(metric_names), (
            f"missing metrics from registry: {expected - metric_names}"
        )

    def test_metric_descriptions_match_expected_text(self, registry, exporter):
        """Verify that each metric has the correct documentation string."""
        # Act
        metrics = self._collect_metrics(registry)

        # Assert
        assert metrics["celery_rate_limiter_consume"].documentation == (
            "Total consume operations performed by the rate limiter."
        ), "consume counter should have the correct description"
        assert metrics["celery_rate_limiter_schedule"].documentation == (
            "Total schedule operations performed by the rate limiter."
        ), "schedule counter should have the correct description"
        assert metrics["celery_rate_limiter_remaining_tokens"].documentation == (
            "Number of rate limit tokens remaining in the current window."
        ), "remaining tokens gauge should have the correct description"
        assert metrics["celery_rate_limiter_active_concurrency"].documentation == (
            "Number of tasks currently executing."
        ), "active concurrency gauge should have the correct description"
        assert metrics["celery_rate_limiter_buffer_depth"].documentation == (
            "Number of tasks waiting in the buffer."
        ), "buffer depth gauge should have the correct description"

    @staticmethod
    def test_consume_counter_label_names(exporter, registry, limiter_id):
        """Verify that the consume counter uses the correct label names."""
        # Arrange
        # Emit one event to materialize label samples in the registry.
        exporter("consume", {
            "success": True, "expired": False, "remaining_tokens": 1,
            "active_concurrency": 0, "reset_in_ms": 100, "remaining_tasks": 0,
        })

        # Act
        label_names = set()
        for metric_family in registry.collect():
            if metric_family.name == "celery_rate_limiter_consume":
                for sample in metric_family.samples:
                    label_names = set(sample.labels.keys())
                    break

        # Assert
        assert label_names == {"limiter_id", "outcome"}, (
            "consume counter should have limiter_id and outcome labels"
        )

    @staticmethod
    def test_schedule_counter_label_names(exporter, registry, limiter_id):
        """Verify that the schedule counter uses the correct label names."""
        # Arrange
        exporter("schedule", {"scheduled": True, "task_id": "t1"})

        # Act
        label_names = set()
        for metric_family in registry.collect():
            if metric_family.name == "celery_rate_limiter_schedule":
                for sample in metric_family.samples:
                    label_names = set(sample.labels.keys())
                    break

        # Assert
        assert label_names == {"limiter_id", "scheduled"}, (
            "schedule counter should have limiter_id and scheduled labels"
        )

    @staticmethod
    def test_gauge_label_names(exporter, registry, limiter_id):
        """Verify that all gauges use only the limiter_id label."""
        # Arrange
        exporter("consume", {
            "success": True, "expired": False, "remaining_tokens": 1,
            "active_concurrency": 0, "reset_in_ms": 100, "remaining_tasks": 0,
        })

        # Act & Assert
        gauge_names = [
            "celery_rate_limiter_remaining_tokens",
            "celery_rate_limiter_active_concurrency",
            "celery_rate_limiter_buffer_depth",
        ]
        for gauge_name in gauge_names:
            for metric_family in registry.collect():
                if metric_family.name == gauge_name:
                    for sample in metric_family.samples:
                        assert set(sample.labels.keys()) == {"limiter_id"}, (
                            f"{gauge_name} should have only the limiter_id label"
                        )
                        break


class TestEdgeCases:
    """Verify graceful handling of unexpected inputs."""

    @staticmethod
    def test_unknown_event_is_ignored(exporter, registry, limiter_id, caplog):
        """Verify that unknown event names do not raise exceptions and emit a debug log."""
        # Act
        with caplog.at_level(logging.DEBUG, logger="celery_rate_limiter.integrations.prometheus"):
            # This invocation must not raise.
            exporter("unknown_event", {"key": "value"})

        # Assert
        matching_records = [
            record for record in caplog.records
            if record.levelname == "DEBUG"
            and limiter_id in record.getMessage()
            and "unknown_event" in record.getMessage()
        ]
        assert len(matching_records) >= 1, (
            "should emit a debug log containing the limiter id and the unknown event name"
        )

    @staticmethod
    def test_custom_registry_isolation():
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
