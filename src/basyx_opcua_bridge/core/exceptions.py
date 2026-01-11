"""
Custom exception hierarchy for basyx-opcua-bridge.
"""

from typing import Any, Optional

class BridgeError(Exception):
    """Base exception for all bridge errors."""
    def __init__(self, message: str, context: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def __str__(self) -> str:
        if self.context:
            ctx_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{self.message} [{ctx_str}]"
        return self.message

class ConnectionError(BridgeError):
    """Raised when OPC UA connection fails."""
    pass

class MappingError(BridgeError):
    """Raised when mapping configuration or execution fails."""
    pass

class TypeConversionError(MappingError):
    """Raised when type conversion between OPC UA and AAS fails."""
    def __init__(self, source_type: str, target_type: str, value: Any, message: Optional[str] = None):
        super().__init__(
            message or f"Cannot convert {source_type} to {target_type}",
            context={"source_type": source_type, "target_type": target_type, "value": repr(value)}
        )

class ValidationError(BridgeError):
    """Raised when input validation fails (e.g., range checks)."""
    def __init__(self, field: str, value: Any, constraint: str, message: Optional[str] = None):
        super().__init__(
            message or f"Validation failed for {field}: {constraint}",
            context={"field": field, "value": repr(value), "constraint": constraint}
        )

class SecurityError(BridgeError):
    """Raised when security operations fail."""
    pass

class SyncError(BridgeError):
    """Raised when synchronization operations fail."""
    pass

class ConflictError(SyncError):
    """Raised when bidirectional sync encounters a conflict."""
    def __init__(self, element_id: str, aas_value: Any, opcua_value: Any, message: Optional[str] = None):
        super().__init__(
            message or f"Conflict detected for {element_id}",
            context={"element_id": element_id, "aas_value": repr(aas_value), "opcua_value": repr(opcua_value)}
        )
