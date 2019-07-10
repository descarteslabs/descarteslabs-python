import operator

import pytest

from descarteslabs.common.graft import client
from descarteslabs.common.graft import interpreter

from ...core import ProxyTypeError
from ...primitives import Int, Str
from .. import List


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic List"):
        List([1, 2])


def test_init():
    lst = List[Int]([1, 2, 3])
    assert client.is_delayed(lst)
    assert interpreter.interpret(
        lst.graft, builtins={"list": lambda *args: list(args)}
    )() == [1, 2, 3]


def test_init_notsequence():
    with pytest.raises(ProxyTypeError, match="Expected an iterable"):
        List[Int](1)


def test_init_wrongtypes():
    with pytest.raises(
        ProxyTypeError,
        match=r"List\[Int\]: Expected iterable values of type .*Int.*, but for item 1, got 'foo'",
    ):
        List[Int]([1, "foo"])


def test_init_copy():
    lst1 = List[Int]([1, 2, 3])
    lst2 = List[Int](lst1)

    assert lst2.graft != lst1.graft
    assert interpreter.interpret(
        lst2.graft, builtins={"list": lambda *args: list(args), "list.copy": list}
    )() == [1, 2, 3]


def test_init_wrong_list_type():
    lst = List[Int]([1, 2, 3])
    with pytest.raises(
        ProxyTypeError,
        match=r"Cannot convert List\[Int\] to List\[Str\], since they have different value types",
    ):
        List[Str](lst)


def test_getitem_type():
    lst = List[Int]([1, 2, 3])
    assert isinstance(lst[0], Int)
    assert isinstance(lst[100], Int)


def test_getitem_roundtrip():
    src = [1, 2, 3]
    tup = List[Int](src)

    for i, truth in enumerate(src):
        value = interpreter.interpret(
            tup[i].graft,
            builtins={"list": lambda *args: list(args), "getitem": operator.getitem},
        )()
        assert value == truth
