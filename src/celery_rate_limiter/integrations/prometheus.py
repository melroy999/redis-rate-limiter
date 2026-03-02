"""Prometheus metrics integration for the distributed rate limiter.

This module provides a ``PrometheusMetricsExporter`` that implements the
``metrics_callback`` signature expected by the rate limiter. It translates
limiter events into Prometheus counters and gauges, enabling observability
through standard Prometheus/Grafana tooling.

Usage::

    from prometheus_client import start_http_server
    from celery_rate_limiter.integrations.prometheus import PrometheusMetricsExporter

    exporter = PrometheusMetricsExporter(limiter_id="my_limiter")
    start_http_server(8000)

    limiter = ThreadPoolRateLimiter.create(
        limiter_id="my_limiter",
        ...,
        metrics_callback=exporter,
    )

Requires the ``prometheus`` extra::

    pip install celery-rate-limiter[prometheus]
"""

from __future__ import annotations

import logging
from typing import Optional

try:
    from prometheus_client import REGISTRY as DEFAULT_REGISTRY
    from prometheus_client import CollectorRegistry, Counter, Gauge
except ImportError as exc:
    raise ImportError(
        "The prometheus_client package is required for the Prometheus integration. "
        "Install it with: pip install celery-rate-limiter[prometheus]"
    ) from exc

logger = logging.getLogger(__name__)


class PrometheusMetricsExporter:
    """Bridge between the rate limiter's metrics callback and Prometheus.

    Instances of this class are callable and match the ``Callable[[str, dict], None]``
    signature expected by the ``metrics_callback`` parameter of the rate limiter
    constructor.

    **Counters** (incremented per event):

    - ``celery_rate_limiter_consume_total{limiter_id, outcome}``:
      total consume operations, where *outcome* is one of ``success``,
      ``rejected``, or ``expired``.
    - ``celery_rate_limiter_schedule_total{limiter_id, scheduled}``:
      total schedule operations, where *scheduled* is ``true`` or ``false``.

    **Gauges** (updated on each consume event):

    - ``celery_rate_limiter_remaining_tokens{limiter_id}``
    - ``celery_rate_limiter_active_concurrency{limiter_id}``
    - ``celery_rate_limiter_buffer_depth{limiter_id}``

    Args:
        limiter_id: The identifier of the rate limiter instance. Used as a
            label value on all emitted metrics.
        registry: An optional Prometheus ``CollectorRegistry``. Defaults to
            the global ``REGISTRY``. Passing a custom registry is useful for
            testing in isolation.
    """

    def __init__(
        self,
        limiter_id: str,
        registry: Optional[CollectorRegistry] = None,
    ) -> None:
        self._limiter_id = limiter_id
        registry = registry or DEFAULT_REGISTRY

        # ---------------------------------------------------------------------------
        # Counters
        # ---------------------------------------------------------------------------

        self._consume_total = Counter(
            "celery_rate_limiter_consume_total",
            "Total consume operations performed by the rate limiter.",
            ["limiter_id", "outcome"],
            registry=registry,
        )

        self._schedule_total = Counter(
            "celery_rate_limiter_schedule_total",
            "Total schedule operations performed by the rate limiter.",
            ["limiter_id", "scheduled"],
            registry=registry,
        )

        # ---------------------------------------------------------------------------
        # Gauges
        # ---------------------------------------------------------------------------

        self._remaining_tokens = Gauge(
            "celery_rate_limiter_remaining_tokens",
            "Number of rate limit tokens remaining in the current window.",
            ["limiter_id"],
            registry=registry,
        )

        self._active_concurrency = Gauge(
            "celery_rate_limiter_active_concurrency",
            "Number of tasks currently executing.",
            ["limiter_id"],
            registry=registry,
        )

        self._buffer_depth = Gauge(
            "celery_rate_limiter_buffer_depth",
            "Number of tasks waiting in the buffer.",
            ["limiter_id"],
            registry=registry,
        )

    # ---------------------------------------------------------------------------
    # Callback interface
    # ---------------------------------------------------------------------------

    def __call__(self, event: str, data: dict) -> None:
        """Handle a metrics event emitted by the rate limiter.

        This method is designed to be passed directly as the
        ``metrics_callback`` argument when constructing a rate limiter.

        Args:
            event: The event name (``"consume"`` or ``"schedule"``).
            data: A dictionary containing event-specific data.
        """
        if event == "consume":
            self._handle_consume(data)
        elif event == "schedule":
            self._handle_schedule(data)
        else:
            logger.debug(
                "Ignoring unknown metrics event: limiter=%s, event=%s.",
                self._limiter_id,
                event,
            )

    # ---------------------------------------------------------------------------
    # Event handlers
    # ---------------------------------------------------------------------------

    def _handle_consume(self, data: dict) -> None:
        """Process a consume event, updating counters and gauges."""
        lid = self._limiter_id

        # Determine the outcome label.
        if data.get("expired"):
            outcome = "expired"
        elif data.get("success"):
            outcome = "success"
        else:
            outcome = "rejected"

        self._consume_total.labels(limiter_id=lid, outcome=outcome).inc()

        # Update point-in-time gauges.
        self._remaining_tokens.labels(limiter_id=lid).set(data["remaining_tokens"])
        self._active_concurrency.labels(limiter_id=lid).set(data["active_concurrency"])
        self._buffer_depth.labels(limiter_id=lid).set(data["remaining_tasks"])

    def _handle_schedule(self, data: dict) -> None:
        """Process a schedule event, updating the schedule counter."""
        scheduled = "true" if data.get("scheduled") else "false"
        self._schedule_total.labels(
            limiter_id=self._limiter_id,
            scheduled=scheduled,
        ).inc()
