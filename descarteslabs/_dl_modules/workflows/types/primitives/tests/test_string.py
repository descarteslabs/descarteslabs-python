import operator

import pytest

from ...containers import Tuple, List
from ..bool_ import Bool
from ..number import Int
from ..string import Str


@pytest.mark.parametrize(
    "other, result_type, op, reflected",
    [
        (Str(""), Str, operator.add, True),
        (Int(1), Str, operator.mul, False),
        (Str(""), Bool, operator.lt, False),
        (Str(""), Bool, operator.le, False),
        (Str(""), Bool, operator.eq, False),
        (Str(""), Bool, operator.ge, False),
        (Str(""), Bool, operator.gt, False),
        (Str(""), Bool, operator.ne, False),
    ],
)
def test_supported_binary_methods(other, result_type, op, reflected):
    assert isinstance(op(Str(""), other), result_type)

    if reflected:
        assert isinstance(op(other, Str("")), result_type)


def test_contains():
    with pytest.raises(TypeError):
        Str("") in Str("")

    assert isinstance(Str("").contains(Str("")), Bool)


def test_reversed():
    assert isinstance(reversed(Str("")), Str)


def test_length():
    with pytest.raises(TypeError):
        len(Str(""))

    assert isinstance(Str("").length(), Int)


@pytest.mark.parametrize(
    "method, return_type, args",
    [
        # custom
        ("contains", Bool, [""]),
        ("length", Int, []),
        # from python
        ("capitalize", Str, []),
        ("center", Str, [1]),
        ("count", Int, [""]),
        # ("decode", Str, []),  # TODO need more types to implement
        # ("encode", Str, []),  # TODO need more types to implement
        ("endswith", Bool, [""]),
        ("expandtabs", Str, []),
        ("find", Int, [""]),
        ("format", Str, [""]),
        ("__getitem__", Str, [0]),
        ("__getitem__", Str, [slice(0, 1, 0)]),
        ("isalnum", Bool, []),
        ("isalpha", Bool, []),
        ("isdigit", Bool, []),
        ("islower", Bool, []),
        ("isspace", Bool, []),
        ("istitle", Bool, []),
        ("isupper", Bool, []),
        ("join", Str, [("a", "b")]),
        ("ljust", Str, [1]),
        ("lower", Str, []),
        ("lstrip", Str, []),
        ("partition", Tuple[Str, Str, Str], [""]),
        ("replace", Str, ["", ""]),
        ("rfind", Int, [""]),
        ("rjust", Str, [1]),
        ("rpartition", Tuple[Str, Str, Str], [""]),
        ("rsplit", List[Str], [""]),
        ("rstrip", Str, []),
        ("split", List[Str], [""]),
        ("splitlines", List[Str], []),
        ("startswith", Bool, [""]),
        ("strip", Str, []),
        ("swapcase", Str, []),
        ("title", Str, []),
        ("upper", Str, []),
        ("zfill", Str, [0]),
    ],
)
def test_has_methods(method, return_type, args):
    s = Str("")
    out = getattr(s, method)(*args)
    assert isinstance(out, return_type)
