from prometheus_client import Counter, Gauge, Histogram, start_http_server
from prometheus_client.registry import CollectorRegistry, REGISTRY

class MetricsCollector:
    """Collector for bridge metrics."""
    
    def __init__(self, port: int = 9090, registry: CollectorRegistry | None = None):
        self.port = port
        self._server_started = False
        self._registry = registry or REGISTRY
        
        # Metrics
        self.sync_events = Counter(
            "bridge_sync_events_total",
            "Total number of synchronization events",
            ["direction", "status"],
            registry=self._registry,
        )
        
        self.active_subscriptions = Gauge(
            "bridge_active_subscriptions",
            "Number of active OPC UA subscriptions",
            registry=self._registry,
        )
        
        self.sync_latency = Histogram(
            "bridge_sync_latency_seconds",
            "Latency of synchronization operations",
            ["direction"],
            registry=self._registry,
        )

    def start_server(self) -> None:
        if not self._server_started:
            start_http_server(self.port, registry=self._registry)
            self._server_started = True

    def record_sync_event(self, direction: str, success: bool) -> None:
        status = "success" if success else "failure"
        self.sync_events.labels(direction=direction, status=status).inc()

    def set_active_subscriptions(self, count: int) -> None:
        self.active_subscriptions.set(count)
