import pytest

from ...core import ProxyTypeError
from ...primitives import Float, Bool, Str, Int
from .. import KnownDict

kd = KnownDict[{"x": Float, "y": Bool}, Str, Int]._from_graft({"returns": "env"})


def test_validate_params():
    KnownDict[Str, Int]
    KnownDict[{"x": Float, "y": Bool}, Str, Int]

    with pytest.raises(TypeError, match="takes 2 or 3 type parameters"):
        KnownDict[Str]
        KnownDict[{"x": Float, "y": True}]
    with pytest.raises(TypeError, match="must be a Proxytype"):
        KnownDict[1, Str]
        KnownDict[{"x": Float, "y": Bool}, "test", Int]


def test_known_key():
    assert isinstance(kd["x"], Float)
    assert isinstance(kd["y"], Bool)


def test_unknown_key():
    assert isinstance(kd["foo"], Int)


def test_proxy_key():
    assert isinstance(kd[Str("bar")], Int)
    assert isinstance(kd[Str("x")], Int)


def test_wrong_keytype():
    with pytest.raises(ProxyTypeError, match="keys are of type"):
        kd[1]
