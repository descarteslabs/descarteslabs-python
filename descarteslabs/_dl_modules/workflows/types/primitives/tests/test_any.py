import operator

import pytest

from ...containers import Tuple
from ...identifier import parameter
from ..any_ import Any
from ..bool_ import Bool
from ..number import Int
from ..string import Str


def test_init():
    assert Any(1).params == ()

    x = parameter("x", Int)
    assert Any(x).params == (x,)


@pytest.mark.parametrize(
    "op, other, return_type, reflected",
    [
        (operator.add, 0, Any, True),
        (operator.add, 0.0, Any, True),
        (operator.eq, 0, Bool, True),
        (operator.floordiv, 0, Any, True),
        (operator.floordiv, 0.0, Any, True),
        (operator.ge, 0, Bool, True),
        (operator.gt, 0, Bool, True),
        (operator.le, 0, Bool, True),
        (operator.lt, 0, Bool, True),
        (operator.mod, 0, Any, True),
        (operator.mul, 0.0, Any, True),
        (operator.ne, 0, Bool, True),
        (operator.pow, 0, Any, True),
        (operator.pow, 0.0, Any, True),
        (operator.sub, 0, Any, True),
        (operator.sub, 0.0, Any, True),
        (operator.truediv, 0, Any, True),
        (operator.truediv, 0.0, Any, True),
        (divmod, 0, Tuple, True),
        (operator.and_, 0, Any, True),
        (operator.lshift, 0, Any, True),
        (operator.or_, 0, Any, True),
        (operator.rshift, 0, Any, True),
        (operator.xor, 0, Any, True),
    ],
)
def test_binary_methods(op, other, return_type, reflected):
    any_ = Any(0)
    assert isinstance(op(any_, other), return_type)

    if reflected:
        assert isinstance(op(other, any_), return_type)


@pytest.mark.parametrize(
    "op, return_type",
    [
        (operator.abs, Any),
        (operator.inv, Any),
        (operator.invert, Any),
        (operator.neg, Any),
        (operator.pos, Any),
        (reversed, Any),
    ],
)
def test_unary_methods(op, return_type):
    any_ = Int(0)._cast(Any)
    assert isinstance(op(any_), return_type)


@pytest.mark.parametrize(
    "op, exception",
    [(operator.truth, TypeError), (operator.index, TypeError), (hex, TypeError)],
)
def test_unsupported_unary_methods(op, exception):
    any_ = Int(0)._cast(Any)
    with pytest.raises(exception):
        op(any_)


def test_contains():
    any_ = Str("")._cast(Any)

    with pytest.raises(TypeError):
        "" in any_

    assert isinstance(any_.contains(""), Bool)


def test_length():
    any_ = Str("")._cast(Any)

    with pytest.raises(TypeError):
        len(any_)

    assert isinstance(any_.length(), Int)


def test_call():
    any_ = Str("")._cast(Any)
    with pytest.raises(TypeError):
        any_()


def test_cast():
    any_ = Any(0).cast(Int)
    assert isinstance(any_, Int)

    any_ = Any((1, 2)).cast(Tuple[Int, Int])
    assert isinstance(any_, Tuple)

    with pytest.raises(AssertionError, match="Cannot instantiate a generic"):
        Any((1, 2)).cast(Tuple)


def test_getters():
    any_ = Str("")._cast(Any)

    assert isinstance(getattr(any_, "foo"), Any)
    assert isinstance(any_[0], Any)

    with pytest.raises(AttributeError):
        # private attributes are blocked
        any_._foo


def test_cant_iter():
    any_ = Any(0)
    with pytest.raises(TypeError, match="Any object is not iterable"):
        iter(any_)
