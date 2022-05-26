import datetime
import operator

import pytest

from ...primitives import Bool, Int, Any
from ..datetime_ import Datetime, _binary_op_casts_to
from ..timedelta import Timedelta


def test_from_string():
    assert isinstance(Datetime.from_string(""), Datetime)


def test_promote():
    assert isinstance(Datetime._promote(datetime.datetime.now()), Datetime)
    assert isinstance(Datetime._promote(datetime.datetime.now().isoformat()), Datetime)
    assert isinstance(
        Datetime._promote(
            (datetime.datetime.now() - datetime.datetime(1970, 1, 1)).total_seconds()
        ),
        Datetime,
    )
    assert isinstance(Datetime._promote(Any(1)), Datetime)
    with pytest.raises(TypeError):
        Datetime._promote({})


@pytest.mark.parametrize("seconds", [0, 0.0])
def test_from_timestamp(seconds):
    assert isinstance(Datetime.from_timestamp(seconds), Datetime)


def test_init():
    assert isinstance(
        Datetime(year=0, month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
        Datetime,
    )


@pytest.mark.parametrize(
    "op, other, return_type",
    [
        (operator.add, Timedelta(hours=0), Datetime),
        (operator.eq, Datetime.from_string(""), Bool),
        (operator.ge, Datetime.from_string(""), Bool),
        (operator.gt, Datetime.from_string(""), Bool),
        (operator.le, Datetime.from_string(""), Bool),
        (operator.lt, Datetime.from_string(""), Bool),
        (operator.ne, Datetime.from_string(""), Bool),
        (operator.sub, Timedelta(hours=0), Datetime),
        (operator.sub, Datetime.from_string(""), Timedelta),
    ],
)
def test_binary_methods(op, other, return_type):
    dt = Datetime.from_string("")
    assert isinstance(op(dt, other), return_type)


def test_binary_op_casts_to():
    td = Timedelta(hours=0)
    dt = Datetime.from_string("")

    _binary_op_casts_to(dt, td) is Datetime
    _binary_op_casts_to(dt, dt) is Timedelta


@pytest.mark.parametrize(
    "field, type_",
    [
        ("year", Int),
        ("month", Int),
        ("day", Int),
        ("hour", Int),
        ("minute", Int),
        ("second", Int),
        ("microsecond", Int),
    ],
)
def test_datetime_struct(field, type_):
    dt = Datetime.from_string("")
    assert isinstance(getattr(dt, field), type_)


def test_is_between():
    dt = Datetime(2018, 1, 2)
    assert isinstance(dt.is_between("2017-01-01", Datetime(2019)), Bool)
