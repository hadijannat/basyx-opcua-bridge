from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Literal, Optional, List

from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

class SecurityPolicy(str, Enum):
    """OPC UA Security Policies."""
    NONE = "None"
    BASIC128RSA15 = "Basic128Rsa15"
    BASIC256 = "Basic256"
    BASIC256SHA256 = "Basic256Sha256"
    AES128_SHA256_RSAOAEP = "Aes128_Sha256_RsaOaep"
    AES256_SHA256_RSAPSS = "Aes256_Sha256_RsaPss"

class MessageSecurityMode(str, Enum):
    """OPC UA Message Security Modes."""
    NONE = "None"
    SIGN = "Sign"
    SIGN_AND_ENCRYPT = "SignAndEncrypt"

class SyncDirection(str, Enum):
    """Synchronization direction for a mapping."""
    OPCUA_TO_AAS = "opcua_to_aas"
    AAS_TO_OPCUA = "aas_to_opcua"
    BIDIRECTIONAL = "bidirectional"

class ConflictResolution(str, Enum):
    """Strategy for resolving bidirectional conflicts."""
    OPCUA_WINS = "opcua_wins"
    AAS_WINS = "aas_wins"
    TIMESTAMP_WINS = "timestamp_wins"
    MANUAL = "manual"

class EndpointConfig(BaseModel):
    """Configuration for a single OPC UA endpoint."""
    url: str = Field(..., description="OPC UA server URL")
    name: str = Field(default="", description="Human-readable name")
    security_policy: SecurityPolicy = Field(default=SecurityPolicy.BASIC256SHA256)
    security_mode: MessageSecurityMode = Field(default=MessageSecurityMode.SIGN_AND_ENCRYPT)
    timeout_ms: int = Field(default=10000, ge=1000)
    username: Optional[str] = None
    password: Optional[str] = None

    @field_validator("url")
    @classmethod
    def validate_opcua_url(cls, v: str) -> str:
        if not v.startswith("opc.tcp://"):
            raise ValueError("OPC UA URL must start with opc.tcp://")
        return v

class OpcUaConfig(BaseModel):
    """OPC UA connection configuration."""
    endpoints: List[EndpointConfig] = Field(..., min_length=1)
    connection_pool_size: int = Field(default=5, ge=1)
    subscription_interval_ms: int = Field(default=500, ge=50)
    monitor_queue_maxsize: int = Field(default=10000, ge=1)
    encoding: Literal["binary", "xml"] = Field(default="binary")

class SecurityConfig(BaseModel):
    """Security configuration."""
    client_certificate_path: Optional[Path] = None
    client_private_key_path: Optional[Path] = None
    trusted_server_certs_dir: Optional[Path] = None
    enable_identity_propagation: bool = True
    audit_log_enabled: bool = True

    @model_validator(mode="after")
    def validate_cert_pair(self) -> SecurityConfig:
        if self.client_certificate_path and not self.client_private_key_path:
            raise ValueError("Private key required when certificate is provided")
        if self.client_private_key_path and not self.client_certificate_path:
            raise ValueError("Certificate required when private key is provided")
        return self

class RangeConstraint(BaseModel):
    """Range constraint for validated writes."""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unit: Optional[str] = None

    @model_validator(mode="after")
    def validate_range(self) -> RangeConstraint:
        if self.min_value is not None and self.max_value is not None:
            if self.min_value > self.max_value:
                raise ValueError("min_value must be <= max_value")
        return self

class MappingRule(BaseModel):
    """Mapping rule between OPC UA node and AAS SubmodelElement."""
    opcua_node_id: str = Field(..., pattern=r"^(ns=\d+;[sigb]=.+|i=\d+)$")
    aas_id_short: str
    submodel_id: str
    endpoint: Optional[str] = Field(default=None, description="Endpoint name or URL to target")
    semantic_id: Optional[str] = None
    direction: SyncDirection = SyncDirection.BIDIRECTIONAL
    value_type: str = "xs:double"
    range_constraint: Optional[RangeConstraint] = None
    transform_expression: Optional[str] = None
    enabled: bool = True

class SemanticConfig(BaseModel):
    eclass_api_url: Optional[HttpUrl] = None
    iri_resolver_url: Optional[HttpUrl] = None
    cache_ttl_seconds: int = 3600

class AasProviderConfig(BaseModel):
    type: Literal["basyx", "aasx-server", "memory"] = "memory"
    url: Optional[HttpUrl] = None
    enable_events: bool = True

class ObservabilityConfig(BaseModel):
    metrics_enabled: bool = True
    metrics_port: int = 9090
    tracing_enabled: bool = False
    tracing_endpoint: Optional[HttpUrl] = None
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

class BridgeConfig(BaseSettings):
    """Root configuration."""
    model_config = SettingsConfigDict(
        env_prefix="BRIDGE_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    opcua: OpcUaConfig
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    mappings: List[MappingRule] = Field(default_factory=list)
    semantic: SemanticConfig = Field(default_factory=SemanticConfig)
    aas: AasProviderConfig = Field(default_factory=AasProviderConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    conflict_resolution: ConflictResolution = ConflictResolution.TIMESTAMP_WINS

    @classmethod
    def from_yaml(cls, path: str | Path) -> BridgeConfig:
        import yaml
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)
