import pytest
from asyncua import ua
from basyx_opcua_bridge.mapping.type_converters import opcua_to_python, python_to_opcua, TypeConversionError

def test_double_conversion():
    res = opcua_to_python(42.5, ua.VariantType.Double.value)
    assert res == 42.5
    assert isinstance(res, float)

def test_python_to_opcua():
    val, vtype = python_to_opcua("42.5", "xs:double")
    assert val == 42.5
    assert vtype == ua.VariantType.Double.value

def test_invalid_type():
    with pytest.raises(TypeConversionError):
        python_to_opcua(42, "xs:unknown")
