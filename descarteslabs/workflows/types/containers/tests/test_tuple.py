import operator

import pytest

from descarteslabs.common.graft import client
from descarteslabs.common.graft import interpreter

from ...core import ProxyTypeError
from ...primitives import Int, Str, Bool
from ...identifier import parameter
from .. import Tuple, List


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic Tuple"):
        Tuple([1, 2])


def test_init_sequence():
    b = parameter("b", Bool)
    tup = Tuple[Int, Str, Int, Bool]([1, "foo", 3, b])
    assert client.is_delayed(tup)
    assert tup.params == (b,)
    assert interpreter.interpret(
        tup.graft, builtins={"wf.tuple": lambda *tuple: tuple, b._name: True}
    )() == (1, "foo", 3, True)


def test_init_iterable():
    seq = [1, "foo", 3]

    def generator():
        for x in seq:
            yield x

    tup = Tuple[Int, Str, Int](generator())
    assert client.is_delayed(tup)
    assert tup.params == ()


def test_init_sequence_wronglength():
    with pytest.raises(
        ProxyTypeError, match="expected an iterable of 3 items, but got 4 items"
    ):
        Tuple[Int, Str, Int]([1, "foo", 3, "woah there"])


def test_init_iterable_wronglength():
    seq = [1, "foo", 3, "woah there"]

    def generator():
        for x in seq:
            yield x

    with pytest.raises(
        ProxyTypeError, match="expected an iterable of 3 items, but got 4 items"
    ):
        Tuple[Int, Str, Int](generator())


def test_init_wrongtypes():
    with pytest.raises(
        ProxyTypeError,
        match=r"While constructing Tuple\[Int, Str, Int\], expected .*Str.* for tuple element 1, but got 2",
    ):
        Tuple[Int, Str, Int]([1, 2, 3])


def test_validate_params():
    Tuple[Str, Int]
    Tuple[List[Int], Str]

    with pytest.raises(TypeError, match="must be Proxytypes"):
        Tuple[1]


def test_getitem_type():
    tup = Tuple[Int, Str, Int]([1, "foo", 3])
    assert isinstance(tup[0], Int)
    assert isinstance(tup[1], Str)
    assert isinstance(tup[2], Int)
    assert isinstance(tup[:2], Tuple)
    with pytest.raises(IndexError):
        tup[4]


def test_getitem_roundtrip():
    src = [1, "foo", 3]
    tup = Tuple[Int, Str, Int](src)

    for i, truth in enumerate(src):
        value = interpreter.interpret(
            tup[i].graft,
            builtins={"wf.tuple": lambda *tuple: tuple, "wf.get": operator.getitem},
        )()
        assert value == truth


def test_len():
    tup = Tuple[Int, Str, Int]([1, "foo", 3])
    assert len(tup) == 3


def test_iter():
    tup = Tuple[Int, Str, Int]([1, "foo", 3])
    itered = list(tup)
    assert isinstance(itered[0], Int)
    assert isinstance(itered[1], Str)
    assert isinstance(itered[2], Int)


@pytest.mark.parametrize(
    "method",
    [operator.lt, operator.le, operator.gt, operator.ge, operator.eq, operator.ne],
)
@pytest.mark.parametrize("other", [Tuple[Int, Str]([2, "foo"]), (2, "foo")])
def test_container_methods(method, other):
    tuple_ = Tuple[Int, Str]([1, "baz"])
    result = method(tuple_, other)
    assert isinstance(result, Bool)


def test_container_methods_check_elem_type():
    tuple_ = Tuple[Bool]([True])
    with pytest.raises(
        TypeError, match=r"Operator `<` invalid for element Bool in Tuple\[Bool\]"
    ):
        tuple_ < tuple_


def test_container_methods_recursive_check():
    tuple_ = Tuple[Tuple[Int, Str], Tuple[Str], Int](((1, "foo"), ("bar",), 2))
    assert isinstance(tuple_ < tuple_, Bool)

    tuple_ = Tuple[Tuple[Bool]]([[True]])
    with pytest.raises(
        TypeError,
        match=r"Operator `<` invalid for element Tuple\[Bool\] in Tuple\[Tuple\[Bool\]\]",
    ):
        tuple_ < tuple_

    tuple_ = Tuple[Tuple[Int], Bool]([[1], True])
    with pytest.raises(
        TypeError,
        match=r"Operator `<` invalid for element Bool in Tuple\[Tuple\[Int\], Bool\]",
    ):
        tuple_ < tuple_


@pytest.mark.parametrize(
    "other",
    [Tuple[Bool, Str, Tuple[Int, Int]]((True, "foo", (1, 2))), (True, "foo", (1, 2))],
)
def test_add(other):
    tuple_ = Tuple[Int, Str]([1, "baz"])
    add = tuple_ + other
    assert isinstance(add, Tuple[Int, Str, Bool, Str, Tuple[Int, Int]])
    radd = other + tuple_
    assert isinstance(radd, Tuple[Bool, Str, Tuple[Int, Int], Int, Str])


def test_add_check():
    with pytest.raises(TypeError):
        Tuple[Int, Str]([1, "baz"]) + "blah"

    with pytest.raises(TypeError):
        [1, 2] + Tuple[Int, Str]([1, "baz"])
