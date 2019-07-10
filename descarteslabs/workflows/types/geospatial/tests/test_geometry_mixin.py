import pytest

from ... import Proxytype
from ..mixins import GeometryMixin


class Foo(Proxytype, GeometryMixin):
    pass


@pytest.mark.parametrize("distance", [0, 0.0])
def test_buffer(distance):
    assert isinstance(Foo().buffer(distance), Foo)
