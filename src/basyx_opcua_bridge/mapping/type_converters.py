from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any, Callable, Optional, Dict

from asyncua import ua
from basyx_opcua_bridge.core.exceptions import TypeConversionError

OPCUA_TO_XSD_MAP: Dict[int, str] = {
    ua.VariantType.Boolean.value: "xs:boolean",
    ua.VariantType.SByte.value: "xs:byte",
    ua.VariantType.Byte.value: "xs:unsignedByte",
    ua.VariantType.Int16.value: "xs:short",
    ua.VariantType.UInt16.value: "xs:unsignedShort",
    ua.VariantType.Int32.value: "xs:int",
    ua.VariantType.UInt32.value: "xs:unsignedInt",
    ua.VariantType.Int64.value: "xs:long",
    ua.VariantType.UInt64.value: "xs:unsignedLong",
    ua.VariantType.Float.value: "xs:float",
    ua.VariantType.Double.value: "xs:double",
    ua.VariantType.String.value: "xs:string",
    ua.VariantType.DateTime.value: "xs:dateTime",
    ua.VariantType.ByteString.value: "xs:base64Binary",
    ua.VariantType.Guid.value: "xs:string",
    ua.VariantType.NodeId.value: "xs:string",
    ua.VariantType.LocalizedText.value: "xs:string",
}

XSD_TO_OPCUA_MAP: Dict[str, int] = {v: k for k, v in OPCUA_TO_XSD_MAP.items()}

def opcua_to_python(value: Any, variant_type: int) -> Any:
    if value is None:
        return None
    if variant_type == ua.VariantType.DateTime.value:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)
    if variant_type == ua.VariantType.ByteString.value:
        if isinstance(value, bytes):
            return base64.b64encode(value).decode("ascii")
        return str(value)
    if variant_type == ua.VariantType.LocalizedText.value:
        if hasattr(value, "Text"):
            return value.Text
        return str(value)
    if variant_type == ua.VariantType.NodeId.value:
        return str(value)
    if variant_type == ua.VariantType.Guid.value:
        return str(value)
    if isinstance(value, (list, tuple)):
        return [opcua_to_python(v, variant_type) for v in value]
    return value

def python_to_opcua(value: Any, xsd_type: str) -> tuple[Any, int]:
    if xsd_type not in XSD_TO_OPCUA_MAP:
        raise TypeConversionError("python", xsd_type, value, f"Unsupported XSD type: {xsd_type}")
    
    variant_type = XSD_TO_OPCUA_MAP[xsd_type]
    try:
        converted = _convert_to_opcua_type(value, xsd_type)
        return converted, variant_type
    except (ValueError, TypeError) as e:
        raise TypeConversionError(type(value).__name__, xsd_type, value, str(e))

def _convert_to_opcua_type(value: Any, xsd_type: str) -> Any:
    if value is None:
        return None
    converters: Dict[str, Callable[[Any], Any]] = {
        "xs:boolean": lambda v: bool(v),
        "xs:byte": int,
        "xs:unsignedByte": int,
        "xs:short": int,
        "xs:unsignedShort": int,
        "xs:int": int,
        "xs:unsignedInt": int,
        "xs:long": int,
        "xs:unsignedLong": int,
        "xs:float": float,
        "xs:double": float,
        "xs:string": str,
        "xs:dateTime": _convert_datetime,
        "xs:base64Binary": _convert_base64,
    }
    converter = converters.get(xsd_type, str)
    return converter(value)

def _convert_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    raise ValueError(f"Cannot convert {type(value)} to datetime")

def _convert_base64(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return base64.b64decode(value)
    raise ValueError(f"Cannot convert {type(value)} to bytes")

class TypeConverter:
    def __init__(self) -> None:
        self._custom_converters: Dict[str, Callable[[Any], Any]] = {}

    def register_converter(self, xsd_type: str, converter: Callable[[Any], Any]) -> None:
        self._custom_converters[xsd_type] = converter

    def to_aas(self, opcua_value: Any, variant_type: int, target_xsd_type: Optional[str] = None) -> tuple[Any, str]:
        xsd_type = target_xsd_type or OPCUA_TO_XSD_MAP.get(variant_type, "xs:string")
        python_value = opcua_to_python(opcua_value, variant_type)
        if xsd_type in self._custom_converters:
            python_value = self._custom_converters[xsd_type](python_value)
        return python_value, xsd_type

    def to_opcua(self, aas_value: Any, xsd_type: str) -> tuple[Any, int]:
        return python_to_opcua(aas_value, xsd_type)
