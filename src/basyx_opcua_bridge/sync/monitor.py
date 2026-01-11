from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TYPE_CHECKING, List

import structlog
from asyncua import Node
from asyncua.common.subscription import DataChangeNotif

if TYPE_CHECKING:
    from basyx_opcua_bridge.core.connection import OpcUaConnectionPool
    from basyx_opcua_bridge.mapping.engine import MappingEngine, ResolvedMapping
    from basyx_opcua_bridge.observability.metrics import MetricsCollector

logger = structlog.get_logger(__name__)

@dataclass
class DataChangeEvent:
    node_id: str
    value: Any
    variant_type: int
    source_timestamp: datetime

class SubscriptionHandler:
    def __init__(self, queue: asyncio.Queue[DataChangeEvent], drop_oldest: bool = True):
        self._queue = queue
        self._drop_oldest = drop_oldest

    def datachange_notification(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        try:
            event = DataChangeEvent(
                node_id=str(node.nodeid),
                value=val,
                variant_type=data.monitored_item.Value.VariantType.value,
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
        subscription_interval_ms: int = 500,
        queue_maxsize: int = 10000,
        drop_oldest: bool = True,
    ):
        self._pool = connection_pool
        self._engine = mapping_engine
        self._metrics = metrics
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
        # Create subscriptions
        for ep in self._pool._endpoints:
            try:
                async with self._pool.get_connection(ep.url) as conn:
                    sub = await conn.create_subscription(period_ms=self._subscription_interval_ms, handler=self._handler)

                    nodes_to_monitor = []
                    for m in mappings:
                        try:
                            node = conn.client.get_node(m.rule.opcua_node_id)
                            nodes_to_monitor.append(node)
                        except Exception:
                            logger.warning("node_resolution_failed", node_id=m.rule.opcua_node_id)

                    if nodes_to_monitor:
                        await sub.subscribe_data_change(nodes_to_monitor)
                        self._metrics.set_active_subscriptions(len(nodes_to_monitor))
                        logger.info("subscription_created", endpoint=ep.url, items=len(nodes_to_monitor))
            except Exception as e:
                logger.error("subscription_setup_failed", endpoint=ep.url, error=str(e))

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
            val, _ = self._engine.transform_to_aas(event.node_id, event.value, event.variant_type)
            mapping = self._engine.get_mapping_for_node(event.node_id)
            if mapping and mapping.element:
                # Safe to assume Property for now since we create them
                if hasattr(mapping.element, 'value'):
                    mapping.element.value = val
                self._metrics.record_sync_event("opcua_to_aas", True)
        except Exception as e:
            logger.error("sync_failed", error=str(e))
            self._metrics.record_sync_event("opcua_to_aas", False)

    async def stop(self) -> None:
        self._running = False
        self._shutdown_event.set()
        if self._task:
            self._task.cancel()
