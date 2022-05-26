import pytest
import numpy as np

from ..bool_ import Bool
from ..number import Int, Float
from ..string import Str

from ...core.tests.utils import operator_test


all_values_to_try = [Bool(False), Int(1), Str("True")]


def test_init():
    b1 = Bool(True)
    assert b1.graft[b1.graft["returns"]] is True
    b2 = Bool(False)
    assert b2.graft[b2.graft["returns"]] is False
    b3 = Bool(np.bool_(True))
    assert b3.graft[b3.graft["returns"]] is True
    b4 = Bool(np.bool_(False))
    assert b4.graft[b4.graft["returns"]] is False


@pytest.mark.parametrize(
    "operator, accepted_types, return_type",
    [
        ["__eq__", (Bool, Int, Float), Bool],
        ["__ne__", (Bool, Int, Float), Bool],
        ["__and__", (Bool, Int), Bool],
        ["__or__", (Bool, Int), Bool],
        ["__xor__", (Bool, Int), Bool],
        ["__rand__", (Bool, Int), Bool],
        ["__ror__", (Bool, Int), Bool],
        ["__rxor__", (Bool, Int), Bool],
    ],
)
def test_all_operators_int(operator, accepted_types, return_type):
    operator_test(Bool(True), all_values_to_try, operator, accepted_types, return_type)


def test_invert():
    assert isinstance(~Bool(True), Bool)


def test_helpful_error():
    b = Bool(True)

    with pytest.raises(TypeError, match="cannot be used in conditionals"):
        b and b
