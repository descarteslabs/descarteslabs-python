import operator

import pytest

from descarteslabs.common.graft import client
from descarteslabs.common.graft import interpreter

from ...core import ProxyTypeError
from ...primitives import Int, Str
from .. import Tuple


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic Tuple"):
        Tuple([1, 2])


def test_init_sequence():
    tup = Tuple[Int, Str, Int]([1, "foo", 3])
    assert client.is_delayed(tup)
    assert interpreter.interpret(
        tup.graft, builtins={"tuple": lambda *tuple: tuple}
    )() == (1, "foo", 3)


def test_init_iterable():
    seq = [1, "foo", 3]

    def generator():
        for x in seq:
            yield x

    tup = Tuple[Int, Str, Int](generator())
    assert client.is_delayed(tup)


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


def test_getitem_type():
    tup = Tuple[Int, Str, Int]([1, "foo", 3])
    assert isinstance(tup[0], Int)
    assert isinstance(tup[1], Str)
    assert isinstance(tup[2], Int)
    with pytest.raises(IndexError):
        tup[4]


def test_getitem_roundtrip():
    src = [1, "foo", 3]
    tup = Tuple[Int, Str, Int](src)

    for i, truth in enumerate(src):
        value = interpreter.interpret(
            tup[i].graft,
            builtins={"tuple": lambda *tuple: tuple, "getitem": operator.getitem},
        )()
        assert value == truth


def test_len():
    tup = Tuple[Int, Str, Int]([1, "foo", 3])
    assert len(tup) == 3
