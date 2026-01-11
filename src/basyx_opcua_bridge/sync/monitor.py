from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TYPE_CHECKING, List

import structlog
from asyncua import Node, ua
from asyncua.common.subscription import DataChangeNotif

if TYPE_CHECKING:
    from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
    from basyx_opcua_bridge.mapping.engine import MappingEngine, ResolvedMapping
    from basyx_opcua_bridge.observability.metrics import MetricsCollector
    from basyx_opcua_bridge.aas.providers import AasProvider

logger = structlog.get_logger(__name__)

@dataclass
class DataChangeEvent:
    node_id: str
    value: Any
    variant_type: int | None
    source_timestamp: datetime

class SubscriptionHandler:
    def __init__(self, queue: asyncio.Queue[DataChangeEvent], drop_oldest: bool = True):
        self._queue = queue
        self._drop_oldest = drop_oldest

    def datachange_notification(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        try:
            node_id = node.nodeid.to_string() if hasattr(node.nodeid, "to_string") else str(node.nodeid)
            variant_type = None
            try:
                data_value = data.monitored_item.Value
                if hasattr(data_value, "Value"):
                    variant = data_value.Value
                    if hasattr(variant, "VariantType"):
                        variant_type = variant.VariantType.value
            except Exception:
                variant_type = None
            event = DataChangeEvent(
                node_id=node_id,
                value=val,
                variant_type=variant_type,
                source_timestamp=data.monitored_item.Value.SourceTimestamp,
            )
            if self._queue.full() and self._drop_oldest:
                try:
                    dropped = self._queue.get_nowait()
                    logger.warning("event_queue_full_drop_oldest", dropped_node_id=dropped.node_id)
                except asyncio.QueueEmpty:
                    pass
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.warning("event_queue_full_drop_newest", node_id=event.node_id)
        except Exception as e:
            logger.error("datachange_handler_error", error=str(e))

class MonitoringManager:
    def __init__(
        self,
        connection_pool: OpcUaConnectionPool,
        mapping_engine: MappingEngine,
        metrics: MetricsCollector,
        aas_provider: AasProvider,
        subscription_interval_ms: int = 500,
        queue_maxsize: int = 10000,
        drop_oldest: bool = True,
    ):
        self._pool = connection_pool
        self._engine = mapping_engine
        self._metrics = metrics
        self._aas = aas_provider
        self._subscription_interval_ms = subscription_interval_ms
        self._queue = asyncio.Queue[DataChangeEvent](maxsize=queue_maxsize)
        self._handler = SubscriptionHandler(self._queue, drop_oldest=drop_oldest)
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._shutdown_event = asyncio.Event()

    async def start(self, mappings: List[ResolvedMapping]) -> None:
        if self._running:
            return
        self._shutdown_event.clear()
        self._task = asyncio.create_task(self.run(mappings, self._shutdown_event))

    async def run(self, mappings: List[ResolvedMapping], shutdown_event: asyncio.Event) -> None:
        if self._running:
            return
        self._running = True
        try:
            await self._setup_subscriptions(mappings)
            await self._process_events(shutdown_event)
        finally:
            self._running = False

    async def _setup_subscriptions(self, mappings: List[ResolvedMapping]) -> None:
        endpoint_map: dict[str, list[ResolvedMapping]] = {}
        endpoints = self._pool.endpoints

        for mapping in mappings:
            if mapping.rule.endpoint:
                try:
                    endpoint_url = self._pool.resolve_endpoint_url(mapping.rule.endpoint)
                    endpoint_map.setdefault(endpoint_url, []).append(mapping)
                except Exception:
                    logger.warning("mapping_endpoint_unknown", endpoint=mapping.rule.endpoint)
                continue

            if len(endpoints) == 1:
                endpoint_map.setdefault(endpoints[0].url, []).append(mapping)
            else:
                for endpoint in endpoints:
                    endpoint_map.setdefault(endpoint.url, []).append(mapping)

        for endpoint_url, endpoint_mappings in endpoint_map.items():
            try:
                async with self._pool.get_connection(endpoint_url) as conn:
                    sub = await conn.create_subscription(period_ms=self._subscription_interval_ms, handler=self._handler)

                    nodes_to_monitor = []
                    for m in endpoint_mappings:
                        try:
                            node = conn.client.get_node(m.rule.opcua_node_id)
                            nodes_to_monitor.append(node)
                        except Exception:
                            logger.warning("node_resolution_failed", node_id=m.rule.opcua_node_id)

                    if nodes_to_monitor:
                        await sub.subscribe_data_change(nodes_to_monitor)
                        self._metrics.set_active_subscriptions(len(nodes_to_monitor))
                        logger.info("subscription_created", endpoint=endpoint_url, items=len(nodes_to_monitor))
            except Exception as e:
                logger.error("subscription_setup_failed", endpoint=endpoint_url, error=str(e))

    async def _process_events(self, shutdown_event: asyncio.Event) -> None:
        while self._running and not shutdown_event.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._process_single_event(event) # Simple processing
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("monitor_loop_error", error=str(e))

    async def _process_single_event(self, event: DataChangeEvent) -> None:
        try:
            variant_type = event.variant_type or self._infer_variant_type(event.value)
            val, _ = self._engine.transform_to_aas(event.node_id, event.value, variant_type)
            mapping = self._engine.get_mapping_for_node(event.node_id)
            if mapping:
                await self._aas.update_property(mapping, val)
                self._metrics.record_sync_event("opcua_to_aas", True)
        except Exception as e:
            logger.error("sync_failed", error=str(e))
            self._metrics.record_sync_event("opcua_to_aas", False)

    def _infer_variant_type(self, value: Any) -> int:
        if isinstance(value, bool):
            return int(ua.VariantType.Boolean.value)
        if isinstance(value, int):
            return int(ua.VariantType.Int64.value)
        if isinstance(value, float):
            return int(ua.VariantType.Double.value)
        if isinstance(value, bytes):
            return int(ua.VariantType.ByteString.value)
        if isinstance(value, str):
            return int(ua.VariantType.String.value)
        if isinstance(value, datetime):
            return int(ua.VariantType.DateTime.value)
        return int(ua.VariantType.String.value)

    async def stop(self) -> None:
        self._running = False
        self._shutdown_event.set()
        if self._task:
            self._task.cancel()
