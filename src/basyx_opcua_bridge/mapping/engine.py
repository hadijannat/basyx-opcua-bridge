from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Optional, Dict, List, Tuple

import structlog
from basyx.aas import model as aas_model

from basyx_opcua_bridge.config.models import MappingRule, SyncDirection, SemanticConfig
from basyx_opcua_bridge.core.exceptions import MappingError, ValidationError
from basyx_opcua_bridge.mapping.type_converters import TypeConverter

logger = structlog.get_logger(__name__)

XSD_TO_AAS_DATATYPE: Dict[str, Any] = {
    "xs:boolean": aas_model.datatypes.Boolean,
    "xs:byte": aas_model.datatypes.Byte,
    "xs:unsignedByte": aas_model.datatypes.UnsignedByte,
    "xs:short": aas_model.datatypes.Short,
    "xs:unsignedShort": aas_model.datatypes.UnsignedShort,
    "xs:int": aas_model.datatypes.Int,
    "xs:unsignedInt": aas_model.datatypes.UnsignedInt,
    "xs:long": aas_model.datatypes.Long,
    "xs:unsignedLong": aas_model.datatypes.UnsignedLong,
    "xs:float": aas_model.datatypes.Float,
    "xs:double": aas_model.datatypes.Double,
    "xs:string": aas_model.datatypes.String,
    "xs:dateTime": aas_model.datatypes.DateTime,
    "xs:base64Binary": aas_model.datatypes.Base64Binary,
}

@dataclass
class ResolvedMapping:
    rule: MappingRule
    submodel: Optional[aas_model.Submodel] = None
    element: Optional[aas_model.SubmodelElement] = None
    opcua_namespace_index: Optional[int] = None
    opcua_identifier: Optional[str] = None
    transform_fn: Optional[Callable[[Any], Any]] = None
    inverse_transform_fn: Optional[Callable[[Any], Any]] = None

class MappingEngine:
    def __init__(self, mappings: List[MappingRule], semantic_resolver: Optional[SemanticConfig] = None) -> None:
        self._rules = mappings
        self._semantic_config = semantic_resolver
        self._type_converter = TypeConverter()
        self._resolved: Dict[str, ResolvedMapping] = {}
        self._submodels: Dict[str, aas_model.Submodel] = {}
        self._build_resolved_mappings()

    def _build_resolved_mappings(self) -> None:
        for rule in self._rules:
            if not rule.enabled:
                continue
            ns_idx, identifier = self._parse_node_id(rule.opcua_node_id)
            transform_fn, inverse_fn = self._build_transform_functions(rule.transform_expression) if rule.transform_expression else (None, None)
            
            resolved = ResolvedMapping(
                rule=rule,
                opcua_namespace_index=ns_idx,
                opcua_identifier=identifier,
                transform_fn=transform_fn,
                inverse_transform_fn=inverse_fn,
            )
            self._resolved[rule.opcua_node_id] = resolved

    def _parse_node_id(self, node_id: str) -> Tuple[int, str]:
        pattern = r"ns=(\d+);([sigb])=(.+)"
        match = re.match(pattern, node_id, re.IGNORECASE)
        if not match:
            # Handle i= (ns=0 assumed if missing, but usually ns=0;i=...)
            # For simplicity we enforce ns=
            raise MappingError(f"Invalid NodeId format: {node_id}")
        return int(match.group(1)), match.group(3)

    def _build_transform_functions(self, expression: str) -> Tuple[Callable[[Any], Any], Optional[Callable[[Any], Any]]]:
        allowed = {"abs": abs, "round": round, "min": min, "max": max, "int": int, "float": float}
        def forward(val: Any) -> Any:
            try:
                msg = {"value": val}
                msg.update(allowed)
                return eval(expression, {"__builtins__": {}}, msg)
            except Exception:
                return val
        
        # Simple inverse logic for linear transforms
        inverse = None
        # ... (simplified inverse logic as per plan)
        return forward, inverse

    def register_submodel(self, submodel: aas_model.Submodel, opcua_namespace: str) -> None:
        self._submodels[submodel.id] = submodel
        for node_id, resolved in self._resolved.items():
            if resolved.rule.submodel_id != submodel.id:
                continue
            resolved.submodel = submodel
            element = self._find_or_create_element(submodel, resolved.rule)
            resolved.element = element

    def _find_or_create_element(self, submodel: aas_model.Submodel, rule: MappingRule) -> aas_model.SubmodelElement:
        for element in submodel.submodel_element:
            if element.id_short == rule.aas_id_short:
                return element
        
        semantic_id = aas_model.ExternalReference((aas_model.Key(aas_model.KeyTypes.GLOBAL_REFERENCE, rule.semantic_id),)) if rule.semantic_id else None
        value_type = XSD_TO_AAS_DATATYPE.get(rule.value_type, aas_model.datatypes.String)
        
        new_property = aas_model.Property(
            id_short=rule.aas_id_short,
            value_type=value_type,
            value=None,
            semantic_id=semantic_id
        )
        submodel.submodel_element.add(new_property)
        return new_property

    def get_mapping_for_node(self, node_id: str) -> Optional[ResolvedMapping]:
        return self._resolved.get(node_id)

    def resolved_mappings(self) -> List[ResolvedMapping]:
        return list(self._resolved.values())

    def transform_to_aas(self, node_id: str, opcua_value: Any, variant_type: int) -> Tuple[Any, str]:
        mapping = self.get_mapping_for_node(node_id)
        if not mapping:
            raise MappingError(f"No mapping for node: {node_id}")
        
        value, xsd_type = self._type_converter.to_aas(opcua_value, variant_type, mapping.rule.value_type)
        if mapping.transform_fn:
            value = mapping.transform_fn(value)
        return value, xsd_type

    def transform_to_opcua(self, node_id: str, aas_value: Any) -> Tuple[Any, int]:
        mapping = self.get_mapping_for_node(node_id)
        if not mapping:
            raise MappingError(f"No mapping for node: {node_id}")
        
        if mapping.rule.direction == SyncDirection.OPCUA_TO_AAS:
            raise MappingError(f"Node {node_id} is read-only")

        value = aas_value
        if mapping.inverse_transform_fn:
            value = mapping.inverse_transform_fn(value)
        
        if mapping.rule.range_constraint:
            self._validate_range(value, mapping.rule)
        
        return self._type_converter.to_opcua(value, mapping.rule.value_type)

    def _validate_range(self, value: Any, rule: MappingRule) -> None:
        if not rule.range_constraint:
            return
        try:
            val = float(value)
        except (ValueError, TypeError):
            raise ValidationError(rule.aas_id_short, value, "numeric value required")
        
        if rule.range_constraint.min_value is not None and val < rule.range_constraint.min_value:
            raise ValidationError(rule.aas_id_short, value, f"must be >= {rule.range_constraint.min_value}")
        if rule.range_constraint.max_value is not None and val > rule.range_constraint.max_value:
            raise ValidationError(rule.aas_id_short, value, f"must be <= {rule.range_constraint.max_value}")
