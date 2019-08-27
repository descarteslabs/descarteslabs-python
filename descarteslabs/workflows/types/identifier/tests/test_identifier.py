import pytest

from ... import Int, Str, List, Datetime
from ..identifier import identifier, parameter


def test_identifier_construction():
    a_key = "some_label"
    my_var = identifier(a_key, Int)

    assert my_var.graft["returns"] == a_key
    assert isinstance(my_var, Int)


def test_identifier_type_error():
    with pytest.raises(TypeError):
        identifier("x", Int) + Str("no good!")


def test_parameter_type_fail():
    with pytest.raises(ValueError):
        parameter("x", List)


def test_parameter_name_fail():
    with pytest.raises(ValueError):
        parameter("1", Int)


def test_datetime_param():
    assert isinstance(parameter("start", Datetime), Datetime)
