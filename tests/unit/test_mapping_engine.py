import pytest
import math
from basyx.aas import model as aas_model
from basyx_opcua_bridge.mapping.engine import MappingEngine, MappingError
from basyx_opcua_bridge.config.models import MappingRule, SyncDirection

def test_mapping_resolution():
    mappings = [
        MappingRule(
            opcua_node_id="ns=1;s=Temp",
            aas_id_short="Temp",
            submodel_id="urn:test",
            value_type="xs:double"
        )
    ]
    engine = MappingEngine(mappings)
    
    mapping = engine.get_mapping_for_node("ns=1;s=Temp")
    assert mapping is not None
    assert mapping.opcua_identifier == "Temp"
    assert mapping.opcua_namespace_index == 1

def test_transform_expression():
    mappings = [
        MappingRule(
            opcua_node_id="ns=1;s=Val",
            aas_id_short="Val",
            submodel_id="urn:test",
            value_type="xs:double",
            transform_expression="value * 2 + 1"
        )
    ]
    engine = MappingEngine(mappings)
    
    # Test opcua -> aas (forward)
    val, _ = engine.transform_to_aas("ns=1;s=Val", 10.0, 11) # 11 = Double
    assert val == 21.0

def test_submodel_registration():
    mappings = [
        MappingRule(
            opcua_node_id="ns=1;s=Temp",
            aas_id_short="Temp",
            submodel_id="urn:test",
            value_type="xs:double"
        )
    ]
    engine = MappingEngine(mappings)
    
    sm = aas_model.Submodel(id_="urn:test")
    engine.register_submodel(sm, "ns=1")
    
    # Check if element was created
    elem = sm.get_referable("Temp")
    assert isinstance(elem, aas_model.Property)
    
    # Check if resolved mapping is linked
    mapping = engine.get_mapping_for_node("ns=1;s=Temp")
    assert mapping.element is elem
