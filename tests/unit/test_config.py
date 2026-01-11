import pytest
from pydantic import ValidationError as PydanticValidationError
from basyx_opcua_bridge.config.models import (
    EndpointConfig, SecurityConfig, RangeConstraint
)

def test_endpoint_validation():
    # Valid
    EndpointConfig(url="opc.tcp://localhost:4840", name="test")
    
    # Invalid URL
    with pytest.raises(PydanticValidationError):
        EndpointConfig(url="http://localhost:4840")

def test_range_constraint_validation():
    # Valid
    RangeConstraint(min_value=0, max_value=100)
    
    # Invalid
    with pytest.raises(PydanticValidationError):
        RangeConstraint(min_value=100, max_value=0)

def test_security_config_validation():
    # Valid (Audit only)
    SecurityConfig(audit_log_enabled=True)
    
    # Invalid (Cert without Key)
    with pytest.raises(PydanticValidationError):
        SecurityConfig(client_certificate_path="/tmp/cert.pem")
