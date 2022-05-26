import datetime
import operator

import pytest

from ...primitives import Float, Bool, Int, Any
from ...identifier import parameter
from ..timedelta import Timedelta
from ..datetime_ import Datetime


def test_promote():
    assert isinstance(Timedelta._promote(datetime.timedelta(hours=1)), Timedelta)
    assert isinstance(Timedelta._promote(Any(1)), Timedelta)
    with pytest.raises(TypeError):
        Timedelta._promote({})


def test_init():
    assert isinstance(
        Timedelta(weeks=0, days=0, hours=0, minutes=0, seconds=0, microseconds=0),
        Timedelta,
    )
    assert isinstance(
        Timedelta(weeks=0.5, days=0, hours=1.0, minutes=0, seconds=0, microseconds=1.0),
        Timedelta,
    )

    assert isinstance(Timedelta(), Timedelta)

    assert Timedelta().params == ()

    x = parameter("x", Int)
    y = parameter("y", Int)
    assert Timedelta(days=x, hours=x, weeks=y).params == (x, y)


def test_total_seconds():
    assert isinstance(Timedelta(hours=0).total_seconds(), Float)


@pytest.mark.parametrize(
    "op, other, return_type, reflected",
    [
        (operator.add, datetime.timedelta(hours=1), Timedelta, True),
        (operator.add, datetime.datetime.now(), Datetime, True),
        (operator.eq, datetime.timedelta(hours=1), Bool, False),
        (operator.floordiv, datetime.timedelta(hours=1), Int, False),
        (operator.floordiv, 2, Timedelta, False),
        (operator.ge, datetime.timedelta(hours=1), Bool, False),
        (operator.gt, datetime.timedelta(hours=1), Bool, False),
        (operator.le, datetime.timedelta(hours=1), Bool, False),
        (operator.lt, datetime.timedelta(hours=1), Bool, False),
        (operator.mod, datetime.timedelta(hours=1), Timedelta, False),
        (operator.mul, 2, Timedelta, True),
        (operator.mul, 2.5, Timedelta, True),
        (operator.ne, datetime.timedelta(hours=1), Bool, False),
        (operator.sub, datetime.timedelta(hours=1), Timedelta, True),
        (operator.truediv, datetime.timedelta(hours=1), Float, False),
        (operator.truediv, 2, Timedelta, False),
        (operator.truediv, 2.2, Timedelta, False),
    ],
)
def test_binary_methods(op, other, return_type, reflected):
    td = Timedelta(hours=1)

    assert isinstance(op(td, other), return_type)

    if reflected:
        assert isinstance(op(other, td), return_type)


@pytest.mark.parametrize(
    "op, return_type",
    [(operator.abs, Timedelta), (operator.pos, Timedelta), (operator.neg, Timedelta)],
)
def test_unary_methods(op, return_type):
    td = Timedelta(hours=1)

    assert isinstance(op(td), return_type)


@pytest.mark.parametrize(
    "field, type_", [("days", Int), ("seconds", Int), ("microseconds", Int)]
)
def test_timedelta_struct(field, type_):
    dt = Timedelta()
    assert isinstance(getattr(dt, field), type_)
