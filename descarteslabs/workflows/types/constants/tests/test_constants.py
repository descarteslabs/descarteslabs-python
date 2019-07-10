import pytest
from .. import e, inf, nan, pi
from ...primitives import Float


# Not testing nan since it's not comparable
@pytest.mark.parametrize(
    "func, expected", [(e, Float), (inf, Float), (nan, Float), (pi, Float)]
)
def test_constants(func, expected):

    assert isinstance(func, expected)
