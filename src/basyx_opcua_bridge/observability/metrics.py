from prometheus_client import Counter, Gauge, Histogram, start_http_server

class MetricsCollector:
    """Collector for bridge metrics."""
    
    def __init__(self, port: int = 9090):
        self.port = port
        self._server_started = False
        
        # Metrics
        self.sync_events = Counter(
            "bridge_sync_events_total",
            "Total number of synchronization events",
            ["direction", "status"]
        )
        
        self.active_subscriptions = Gauge(
            "bridge_active_subscriptions",
            "Number of active OPC UA subscriptions"
        )
        
        self.sync_latency = Histogram(
            "bridge_sync_latency_seconds",
            "Latency of synchronization operations",
            ["direction"]
        )

    def start_server(self) -> None:
        if not self._server_started:
            start_http_server(self.port)
            self._server_started = True

    def record_sync_event(self, direction: str, success: bool) -> None:
        status = "success" if success else "failure"
        self.sync_events.labels(direction=direction, status=status).inc()
