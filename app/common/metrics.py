"""
Prometheus metrics collection for observability.
Exports metrics for: HTTP requests, Kafka consumer, MongoDB latency, business events.
"""

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    start_http_server,
    CollectorRegistry,
)
from typing import Optional


class TaskAuraMetrics:
    """Centralized metrics collection for Task Aura services."""

    def __init__(self, service_name: str, registry: Optional[CollectorRegistry] = None):
        """
        Initialize metrics for a service.

        Args:
            service_name: Name of the service (e.g., "customer-facing", "customer-mgmt")
            registry: Prometheus CollectorRegistry (uses default if None)
        """
        self.service_name = service_name
        self.registry = registry if registry is not None else CollectorRegistry()

        # HTTP request metrics
        self.http_request_duration = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            ["method", "endpoint", "status"],
            registry=self.registry,
        )
        self.http_request_total = Counter(
            "http_requests_total",
            "Total HTTP requests",
            ["method", "endpoint", "status"],
            registry=self.registry,
        )
        self.http_request_size = Histogram(
            "http_request_size_bytes",
            "HTTP request body size",
            ["method", "endpoint"],
            registry=self.registry,
        )
        self.http_response_size = Histogram(
            "http_response_size_bytes",
            "HTTP response body size",
            ["method", "endpoint"],
            registry=self.registry,
        )

        # Business metrics
        self.purchases_total = Counter(
            "purchases_total",
            "Total purchases processed",
            ["status"],  # success, failed
            registry=self.registry,
        )
        self.purchase_amount = Histogram(
            "purchase_amount_usd",
            "Purchase amount in USD",
            ["currency"],
            registry=self.registry,
        )

        # Kafka consumer metrics
        self.kafka_messages_consumed = Counter(
            "kafka_messages_consumed_total",
            "Total messages consumed from Kafka",
            ["topic", "status"],  # status: success, failed
            registry=self.registry,
        )
        self.kafka_consumer_lag = Gauge(
            "kafka_consumer_lag",
            "Current consumer lag per partition",
            ["topic", "partition"],
            registry=self.registry,
        )
        self.kafka_batch_size = Histogram(
            "kafka_batch_size",
            "Messages per batch written to MongoDB",
            ["topic"],
            registry=self.registry,
        )

        # MongoDB metrics
        self.mongodb_query_duration = Histogram(
            "mongodb_query_duration_seconds",
            "MongoDB query latency",
            ["operation", "collection"],  # operation: find, insert, etc.
            registry=self.registry,
        )
        self.mongodb_connection_pool = Gauge(
            "mongodb_connection_pool_current",
            "Current MongoDB connection pool size",
            registry=self.registry,
        )

        # System/reliability metrics
        self.service_health = Gauge(
            "service_health",
            "Service health status (1=healthy, 0=unhealthy)",
            ["service"],
            registry=self.registry,
        )
        self.dependency_health = Gauge(
            "dependency_health",
            "Dependency health (1=up, 0=down)",
            ["service", "dependency"],
            registry=self.registry,
        )

    def set_service_health(self, healthy: bool):
        """Set service health status."""
        self.service_health.labels(service=self.service_name).set(1 if healthy else 0)

    def set_dependency_health(self, dependency: str, healthy: bool):
        """Set dependency health status."""
        self.dependency_health.labels(
            service=self.service_name, dependency=dependency
        ).set(1 if healthy else 0)


# Global metrics instance - will be initialized per service
_metrics: Optional[TaskAuraMetrics] = None


def init_metrics(
    service_name: str, registry: Optional[CollectorRegistry] = None
) -> TaskAuraMetrics:
    """Initialize global metrics instance."""
    global _metrics
    if registry is None:
        registry = CollectorRegistry()
    _metrics = TaskAuraMetrics(service_name, registry)
    return _metrics


def get_metrics() -> TaskAuraMetrics:
    """Get the global metrics instance."""
    if _metrics is None:
        raise RuntimeError("Metrics not initialized. Call init_metrics() first.")
    return _metrics


def start_metrics_server(port: int = 8000, registry: Optional[CollectorRegistry] = None):
    """Start Prometheus metrics HTTP server on specified port."""
    if registry is None:
        raise RuntimeError("Metrics registry is required. Initialize metrics first.")
    start_http_server(port, registry=registry)
