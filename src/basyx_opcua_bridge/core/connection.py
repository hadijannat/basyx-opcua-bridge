from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Dict, List, TYPE_CHECKING

from asyncua import Client, ua
from asyncua.common.subscription import Subscription
import structlog

from basyx_opcua_bridge.core.exceptions import ConnectionError
from basyx_opcua_bridge.config.models import EndpointConfig
from basyx_opcua_bridge.security.x509 import CertificateManager

if TYPE_CHECKING:
    pass

logger = structlog.get_logger(__name__)

@dataclass
class PooledConnection:
    client: Client
    endpoint: EndpointConfig
    subscriptions: List[Subscription] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def is_connected(self) -> bool:
        try:
            return self.client.uaclient is not None and self.client.uaclient.protocol is not None
        except Exception:
            return False

    async def create_subscription(self, period_ms: int = 500, handler: object = None) -> Subscription:
        async with self._lock:
            sub = await self.client.create_subscription(period=period_ms, handler=handler)
            self.subscriptions.append(sub)
            return sub

    async def close_all_subscriptions(self) -> None:
        async with self._lock:
            for sub in self.subscriptions:
                try:
                    await sub.delete()
                except Exception:
                    pass
            self.subscriptions.clear()

class OpcUaConnectionPool:
    def __init__(self, endpoints: List[EndpointConfig], cert_manager: CertificateManager, pool_size: int = 5):
        self._endpoints = endpoints
        self._cert_manager = cert_manager
        self._pool_size = pool_size
        self._connections: Dict[str, PooledConnection] = {}
        self._lock = asyncio.Lock()

    @property
    def endpoints(self) -> List[EndpointConfig]:
        return list(self._endpoints)

    def resolve_endpoint_url(self, endpoint_ref: str | None) -> str:
        if endpoint_ref is None:
            if not self._endpoints:
                raise ConnectionError("No OPC UA endpoints configured")
            return self._endpoints[0].url

        for endpoint in self._endpoints:
            if endpoint.url == endpoint_ref or endpoint.name == endpoint_ref:
                return endpoint.url
        raise ConnectionError(f"Unknown endpoint reference: {endpoint_ref}")

    async def connect(self) -> None:
        tasks = [self._connect_endpoint(ep) for ep in self._endpoints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful = sum(1 for r in results if not isinstance(r, Exception))
        if successful == 0:
            raise ConnectionError(f"Failed to connect to any of {len(self._endpoints)} endpoints")
        logger.info("connection_pool_ready", connected=successful, total=len(self._endpoints))

    async def _connect_endpoint(self, endpoint: EndpointConfig) -> None:
        client = Client(url=endpoint.url, timeout=endpoint.timeout_ms / 1000)
        policy_name = endpoint.security_policy.value if hasattr(endpoint.security_policy, "value") else str(endpoint.security_policy)
        if policy_name != "None":
            await self._configure_security(client, endpoint, policy_name)
        
        await client.connect()
        
        async with self._lock:
            old = self._connections.get(endpoint.url)
            if old:
                try:
                    await old.close_all_subscriptions()
                    await old.client.disconnect()
                except Exception:
                    pass
            self._connections[endpoint.url] = PooledConnection(client=client, endpoint=endpoint)
        logger.info("endpoint_connected", endpoint=endpoint.name, url=endpoint.url)

    async def _configure_security(self, client: Client, endpoint: EndpointConfig, policy_name: str) -> None:
        policy = getattr(ua.SecurityPolicyType, policy_name, None)
        if policy is None:
            raise ConnectionError(f"Unsupported security policy: {policy_name}")
        mode_name = endpoint.security_mode.value if hasattr(endpoint.security_mode, "value") else str(endpoint.security_mode)
        mode = getattr(ua.MessageSecurityMode, mode_name, ua.MessageSecurityMode.SignAndEncrypt)
        await client.set_security(
            policy=policy,
            certificate=str(self._cert_manager.client_cert_path) if self._cert_manager.client_cert_path else None,
            private_key=str(self._cert_manager.client_key_path) if self._cert_manager.client_key_path else None,
            mode=mode
        )

    @asynccontextmanager
    async def get_connection(self, endpoint_url: str) -> AsyncIterator[PooledConnection]:
        async with self._lock:
            conn = self._connections.get(endpoint_url)
            if not conn:
                endpoint = next((ep for ep in self._endpoints if ep.url == endpoint_url), None)
                if not endpoint:
                    raise ConnectionError(f"No endpoint for {endpoint_url}")
                await self._connect_endpoint(endpoint)
                conn = self._connections.get(endpoint_url)
        
        if conn and not conn.is_connected:
            await self._connect_endpoint(conn.endpoint)
            conn = self._connections.get(endpoint_url)
        
        if not conn:
            raise ConnectionError(f"No connection for {endpoint_url}")
        yield conn

    async def maintain_connections(self, shutdown_event: asyncio.Event, interval_seconds: float = 5.0) -> None:
        try:
            while not shutdown_event.is_set():
                await asyncio.sleep(interval_seconds)
                await self._reconnect_if_needed()
        except asyncio.CancelledError:
            return

    async def _reconnect_if_needed(self) -> None:
        for endpoint in self._endpoints:
            async with self._lock:
                conn = self._connections.get(endpoint.url)
            if conn is None or not conn.is_connected:
                try:
                    await self._connect_endpoint(endpoint)
                except Exception as e:
                    logger.warning("endpoint_reconnect_failed", endpoint=endpoint.url, error=str(e))

    async def disconnect(self) -> None:
        async with self._lock:
            for conn in self._connections.values():
                try:
                    await conn.close_all_subscriptions()
                    await conn.client.disconnect()
                except Exception:
                    pass
            self._connections.clear()
