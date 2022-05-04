import operator

import pytest

from .....common.graft import client
from .....common.graft import interpreter

from ...core import ProxyTypeError
from ...primitives import Int, Str, Bool, NoneType
from ...identifier import parameter
from .. import List, Tuple


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic List"):
        List([1, 2])


def test_init():
    lst = List[Int]([1, 2, 3])
    assert client.is_delayed(lst)
    assert lst.params == ()
    assert interpreter.interpret(
        lst.graft, builtins={"wf.list": lambda *args: list(args)}
    )() == [1, 2, 3]


def test_init_merge_params():
    x = parameter("x", Int)
    y = parameter("y", Int)

    lst = List[Int]([1, x, 2, y, 3, x])
    assert lst.params == (x, y)


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
    assert lst2.params is lst1.params
    assert interpreter.interpret(
        lst2.graft, builtins={"wf.list": lambda *args: list(args), "wf.list.copy": list}
    )() == [1, 2, 3]


def test_init_wrong_list_type():
    lst = List[Int]([1, 2, 3])
    with pytest.raises(
        ProxyTypeError,
        match=r"Cannot convert List\[Int\] to List\[Str\], since they have different value types",
    ):
        List[Str](lst)


def test_validate_params():
    List[Int]
    List[List[Tuple[Str, Int]]]

    with pytest.raises(AssertionError, match="only have one element type specified"):
        List[Str, Int]
    with pytest.raises(TypeError, match="must be a Proxytype"):
        List[1]
        List[List["test"]]


def test_getitem_type():
    lst = List[Int]([1, 2, 3])
    assert isinstance(lst[0], Int)
    assert isinstance(lst[100], Int)
    assert isinstance(lst[:1], List[Int])


def test_getitem_roundtrip():
    src = [1, 2, 3]
    tup = List[Int](src)

    for i, truth in enumerate(src):
        value = interpreter.interpret(
            tup[i].graft,
            builtins={
                "wf.list": lambda *args: list(args),
                "wf.get": operator.getitem,
            },
        )()
        assert value == truth


@pytest.mark.parametrize(
    "method",
    [operator.lt, operator.le, operator.gt, operator.ge, operator.eq, operator.ne],
)
@pytest.mark.parametrize("other", [List[Int]([2, 3, 4]), [2, 3, 4]])
def test_container_methods(method, other):
    list_ = List[Int]([1, 2, 3])
    result = method(list_, other)
    assert isinstance(result, Bool)


def test_container_methods_check_elem_type():
    list_ = List[NoneType]([])
    with pytest.raises(TypeError, match=r"Operator `<` invalid for List\[NoneType\]"):
        list_ < list_


def test_container_methods_recursive_check():
    list_ = List[List[Int]]([[1], [2]])
    assert isinstance(list_ < list_, Bool)

    list_ = List[List[NoneType]]([[None], [None]])
    with pytest.raises(
        TypeError, match=r"Operator `<` invalid for List\[List\[NoneType\]\]"
    ):
        list_ < list_


@pytest.mark.parametrize("other", [List[Int]([2, 3, 4]), [2, 3, 4]])
def test_add(other):
    list_ = List[Int]([1, 2, 3])
    assert isinstance(list_ + other, type(list_))
    assert isinstance(other + list_, type(list_))


def test_add_check():
    with pytest.raises(TypeError, match="promoting"):
        List[Int]([1, 2, 3]) + ["a", "b", "c"]


@pytest.mark.parametrize("other", [3, Int(3)])
def test_mul(other):
    list_ = List[Int]([1, 2, 3])
    assert isinstance(list_ * other, type(list_))
    assert isinstance(other * list_, type(list_))


def test_mul_check():
    with pytest.raises(TypeError, match="promoting"):
        List[Int]([1, 2, 3]) * 5.5
