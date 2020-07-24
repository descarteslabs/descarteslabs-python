import pytest

from ...core import ProxyTypeError
from .. import Primitive, Int, Float, Bool, Str, NoneType, Any


def test_construct_generic_not_allowed():
    with pytest.raises(TypeError, match="Cannot instantiate a generic Primitive"):
        Primitive(1)


@pytest.mark.parametrize(
    "val, proxytype",
    [(1, Int), (1.0, Float), (False, Bool), ("foo", Str), (None, NoneType)],
)
def test_construct_from_python_primitive(val, proxytype):
    # not much of a test. just making sure nothing raises.
    proxytype(val)


def test_construct_from_wrong_python_primitive():
    with pytest.raises(ProxyTypeError, match="Cannot promote"):
        Int(1.0)


def test_constructor_from_own_type():
    proxy = Float(1.0)
    proxy2 = Float(proxy)
    assert proxy2.graft == proxy.graft


def test_construct_from_unsupported_proxy_type():
    with pytest.raises(ProxyTypeError, match="Cannot promote"):
        Int(NoneType(None))


def test_constructor_from_any():
    proxy = Any(None)
    proxy2 = Float(proxy)
    assert proxy2.graft == proxy.graft
