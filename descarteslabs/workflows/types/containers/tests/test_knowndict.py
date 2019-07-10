import pytest

from ...core import ProxyTypeError
from ...primitives import Float, Bool, Str, Int
from .. import KnownDict

kd = KnownDict[{"x": Float, "y": Bool}, Str, Int]._from_graft({"returns": "env"})


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
