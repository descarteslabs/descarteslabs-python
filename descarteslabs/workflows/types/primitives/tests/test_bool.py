import pytest

from ..bool_ import Bool
from ..number import Int
from ..string import Str

from ...core.tests.utils import operator_test


all_values_to_try = [Bool(False), Int(1), Str("True")]


@pytest.mark.parametrize(
    "operator, accepted_types, return_type",
    [
        ["__eq__", (Bool,), Bool],
        ["__ne__", (Bool,), Bool],
        ["__and__", (Bool,), Bool],
        ["__or__", (Bool,), Bool],
        ["__xor__", (Bool,), Bool],
        ["__rand__", (Bool,), Bool],
        ["__ror__", (Bool,), Bool],
        ["__rxor__", (Bool,), Bool],
    ],
)
def test_all_operators_int(operator, accepted_types, return_type):
    operator_test(Bool(True), all_values_to_try, operator, accepted_types, return_type)


def test_invert():
    assert isinstance(~Bool(True), Bool)


def test_helpful_error():
    b = Bool(True)

    with pytest.raises(TypeError, match="Instead, use bitwise operators"):
        b and b

    with pytest.raises(TypeError, match="Instead, use bitwise operators"):
        b.__nonzero__()  # extra sure for py2
