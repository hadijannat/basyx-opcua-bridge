from __future__ import annotations

import json
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Literal

import structlog
import yaml
from asyncua import Client, Node, ua
from basyx.aas import model as aas_model
from basyx.aas.adapter.json import json_serialization

from basyx_opcua_bridge.config.models import (
    AasProviderConfig,
    BridgeConfig,
    EndpointConfig,
    MappingRule,
    MessageSecurityMode,
    OpcUaConfig,
    RangeConstraint,
    SecurityConfig,
    SecurityPolicy,
    SyncDirection,
)
from basyx_opcua_bridge.mapping.engine import XSD_TO_AAS_DATATYPE
from basyx_opcua_bridge.mapping.type_converters import OPCUA_TO_XSD_MAP

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class DiscoveredNode:
    node_id: str
    browse_name: str
    browse_path: Tuple[str, ...]
    display_name: str
    namespace_index: int
    variant_type: int | None
    xsd_type: str
    writable: bool
    value_rank: int | None
    range_constraint: RangeConstraint | None
    confidence: float


@dataclass(frozen=True)
class MappingDecision:
    node_id: str
    browse_path: str
    aas_id_short: str
    submodel_id: str
    value_type: str
    direction: str
    writable: bool
    confidence: float
    notes: List[str]


@dataclass(frozen=True)
class DiscoveryOptions:
    endpoint_url: str
    endpoint_name: str
    namespaces: Optional[Sequence[int]] = None
    include_pattern: Optional[str] = None
    exclude_pattern: Optional[str] = None
    max_depth: int = 5
    root_node: str = "Objects"
    group_strategy: str = "namespace"  # namespace | path | root
    aas_type: Literal["basyx", "aasx-server", "memory"] = "memory"
    aas_url: Optional[str] = None
    poll_interval_seconds: float = 1.0
    encode_identifiers: bool = True


@dataclass
class DiscoveryResult:
    config: BridgeConfig
    mappings: List[MappingRule]
    report: Dict[str, Any]


async def discover_opcua(options: DiscoveryOptions) -> DiscoveryResult:
    logger.info("discovery_starting", endpoint=options.endpoint_url)
    nodes = await _discover_nodes(options)
    mappings, decisions = _build_mappings(nodes, options)

    aas_config = AasProviderConfig.model_validate(
        {
            "type": options.aas_type,
            "url": options.aas_url,
            "enable_events": True,
            "poll_interval_seconds": options.poll_interval_seconds,
            "encode_identifiers": options.encode_identifiers,
        }
    )

    config = BridgeConfig(
        opcua=OpcUaConfig(
            endpoints=[
                EndpointConfig(
                    url=options.endpoint_url,
                    name=options.endpoint_name,
                    security_policy=SecurityPolicy.NONE,
                    security_mode=MessageSecurityMode.NONE,
                )
            ],
            subscription_interval_ms=500,
            connection_pool_size=1,
        ),
        security=SecurityConfig(),
        mappings=mappings,
        aas=aas_config,
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "endpoint": options.endpoint_url,
        "mappings": [decision.__dict__ for decision in decisions],
    }

    logger.info("discovery_completed", mappings=len(mappings))
    return DiscoveryResult(config=config, mappings=mappings, report=report)


def write_bridge_config(config: BridgeConfig, out_path: Path) -> None:
    payload = {
        "opcua": config.opcua.model_dump(exclude_none=True),
        "mappings": [m.model_dump(exclude_none=True) for m in config.mappings],
        "aas": config.aas.model_dump(exclude_none=True),
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def generate_aas_env_json(mappings: Iterable[MappingRule]) -> Dict[str, Any]:
    submodels: Dict[str, aas_model.Submodel] = {}
    for mapping in mappings:
        submodel = submodels.get(mapping.submodel_id)
        if not submodel:
            submodel = aas_model.Submodel(id_=mapping.submodel_id)
            submodels[mapping.submodel_id] = submodel

        value_type = XSD_TO_AAS_DATATYPE.get(mapping.value_type, aas_model.datatypes.String)
        element = aas_model.Property(
            id_short=mapping.aas_id_short,
            value_type=value_type,
            value=None,
        )
        submodel.submodel_element.add(element)

    return {
        "submodels": [json.loads(json.dumps(sm, cls=json_serialization.AASToJsonEncoder)) for sm in submodels.values()],
        "assetAdministrationShells": [],
        "conceptDescriptions": [],
    }


async def _discover_nodes(options: DiscoveryOptions) -> List[DiscoveredNode]:
    include = re.compile(options.include_pattern) if options.include_pattern else None
    exclude = re.compile(options.exclude_pattern) if options.exclude_pattern else None

    client = Client(url=options.endpoint_url)
    await client.connect()
    try:
        root = _resolve_root_node(client, options.root_node)
        queue: deque[Tuple[Node, int, Tuple[str, ...]]] = deque([(root, 0, tuple())])
        visited: set[str] = set()
        results: List[DiscoveredNode] = []

        while queue:
            node, depth, path = queue.popleft()
            try:
                node_id = node.nodeid.to_string() if hasattr(node.nodeid, "to_string") else str(node.nodeid)
            except Exception:
                node_id = str(node.nodeid)

            if node_id in visited:
                continue
            visited.add(node_id)

            try:
                browse_name = await node.read_browse_name()
                browse_name_str = browse_name.Name
            except Exception:
                browse_name_str = node_id

            current_path = path + (browse_name_str,)
            path_str = "/".join(current_path)

            try:
                node_class = await node.read_node_class()
            except Exception:
                node_class = None

            if node_class == ua.NodeClass.Variable:
                namespace_index = node.nodeid.NamespaceIndex if hasattr(node.nodeid, "NamespaceIndex") else 0
                if options.namespaces and namespace_index not in options.namespaces:
                    continue
                if include and not include.search(path_str):
                    continue
                if exclude and exclude.search(path_str):
                    continue

                variant_type = await _read_variant_type(node)
                xsd_type = OPCUA_TO_XSD_MAP.get(variant_type or -1, "xs:string")
                writable = await _is_writable(node)
                value_rank = await _read_value_rank(node)
                range_constraint = await _read_range_constraint(node)

                confidence = 1.0 if variant_type is not None else 0.5

                results.append(
                    DiscoveredNode(
                        node_id=node_id,
                        browse_name=browse_name_str,
                        browse_path=current_path,
                        display_name=browse_name_str,
                        namespace_index=namespace_index,
                        variant_type=variant_type,
                        xsd_type=xsd_type,
                        writable=writable,
                        value_rank=value_rank,
                        range_constraint=range_constraint,
                        confidence=confidence,
                    )
                )

            if depth < options.max_depth:
                try:
                    children = await node.get_children()
                except Exception:
                    children = []
                for child in children:
                    queue.append((child, depth + 1, current_path))

        results.sort(key=lambda item: item.node_id)
        return results
    finally:
        await client.disconnect()


def _resolve_root_node(client: Client, root: str) -> Node:
    if root.lower() in {"objects", "objectsfolder"}:
        return client.get_node(ua.ObjectIds.ObjectsFolder)
    try:
        node_id = ua.NodeId.from_string(root)
        return client.get_node(node_id)
    except Exception:
        return client.get_node(ua.ObjectIds.ObjectsFolder)


def _build_mappings(nodes: List[DiscoveredNode], options: DiscoveryOptions) -> Tuple[List[MappingRule], List[MappingDecision]]:
    mappings: List[MappingRule] = []
    decisions: List[MappingDecision] = []
    id_short_counts: Dict[str, Dict[str, int]] = {}

    for node in nodes:
        submodel_id = _resolve_submodel_id(node, options)
        id_short_counts.setdefault(submodel_id, {})

        base_id_short = _sanitize_id_short(node.browse_name)
        count = id_short_counts[submodel_id].get(base_id_short, 0) + 1
        id_short_counts[submodel_id][base_id_short] = count
        aas_id_short = base_id_short if count == 1 else f"{base_id_short}_{count}"

        direction = SyncDirection.BIDIRECTIONAL if node.writable else SyncDirection.OPCUA_TO_AAS

        mapping = MappingRule(
            opcua_node_id=node.node_id,
            aas_id_short=aas_id_short,
            submodel_id=submodel_id,
            value_type=node.xsd_type,
            direction=direction,
            range_constraint=node.range_constraint,
            endpoint=options.endpoint_name,
        )
        mappings.append(mapping)

        notes: List[str] = []
        if node.value_rank not in (None, -1, 0):
            notes.append(f"value_rank={node.value_rank}")
        if count > 1:
            notes.append("id_short_collision")

        decisions.append(
            MappingDecision(
                node_id=node.node_id,
                browse_path="/".join(node.browse_path),
                aas_id_short=aas_id_short,
                submodel_id=submodel_id,
                value_type=node.xsd_type,
                direction=direction.value,
                writable=node.writable,
                confidence=node.confidence,
                notes=notes,
            )
        )

    return mappings, decisions


def _resolve_submodel_id(node: DiscoveredNode, options: DiscoveryOptions) -> str:
    group = "root"
    if options.group_strategy == "namespace":
        group = f"ns{node.namespace_index}"
    elif options.group_strategy == "path":
        if len(node.browse_path) > 1:
            group = "-".join(node.browse_path[:-1])
    elif options.group_strategy == "root":
        group = "root"

    group = _sanitize_segment(group)
    return f"urn:opcua:{options.endpoint_name}:{group}"


def _sanitize_id_short(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", value)
    if not sanitized:
        sanitized = "Property"
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    return sanitized


def _sanitize_segment(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_\-]", "-", value)
    return sanitized or "root"


async def _read_variant_type(node: Node) -> int | None:
    try:
        variant = await node.read_data_type_as_variant_type()
        return int(variant.value)
    except Exception:
        return None


def _has_write_bit(access_level: int) -> bool:
    shift_mask = 1 << int(ua.AccessLevel.CurrentWrite.value)
    direct_mask = int(ua.AccessLevel.CurrentWrite)
    return bool(access_level & shift_mask or access_level & direct_mask)


async def _is_writable(node: Node) -> bool:
    for attr_id in (ua.AttributeIds.UserAccessLevel, ua.AttributeIds.AccessLevel):
        access = await _read_access_level_attr(node, attr_id)
        if access is not None and _has_write_bit(access):
            return True
    getters = [node.get_user_access_level, node.get_access_level]
    for getter in getters:
        try:
            access = await getter()
        except Exception:
            continue
        try:
            if _has_write_bit(int(access)):
                return True
        except Exception:
            continue
    return False


async def _read_access_level_attr(node: Node, attr_id: ua.AttributeIds) -> int | None:
    try:
        data_value = await node.read_attribute(attr_id)
    except Exception:
        return None
    if data_value is None:
        return None
    value = getattr(data_value, "Value", None)
    if value is None:
        return None
    if hasattr(value, "Value"):
        try:
            return int(value.Value)
        except Exception:
            return None
    try:
        return int(value)
    except Exception:
        return None


async def _read_value_rank(node: Node) -> int | None:
    try:
        return int(await node.read_value_rank())
    except Exception:
        return None


async def _read_range_constraint(node: Node) -> RangeConstraint | None:
    try:
        eurange_node = await node.get_child(["0:EURange"])
    except Exception:
        return None

    try:
        eurange = await eurange_node.read_value()
    except Exception:
        return None

    low = getattr(eurange, "Low", None)
    high = getattr(eurange, "High", None)
    if low is None and high is None:
        return None
    return RangeConstraint(min_value=low, max_value=high)
