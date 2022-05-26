import operator

import pytest

from ...primitives import Int, Float, Bool

from .._check_valid_binop import check_valid_binop_for


def test_valid():
    check_valid_binop_for(operator.add, Int, "While testing", valid_result_types=(Int,))


def test_unsupported():
    with pytest.raises(
        TypeError, match="While testing: Bool does not support operator add"
    ):
        check_valid_binop_for(operator.add, Bool, "While testing")


def test_invalid():
    with pytest.raises(
        TypeError,
        match="While testing: operator and_ on Bool produces type Bool. Must produce one of: Int",
    ):
        check_valid_binop_for(
            operator.and_, Bool, "While testing", valid_result_types=(Int,)
        )


def test_unsupported_custom_msg():
    with pytest.raises(TypeError, match="Bool add"):
        check_valid_binop_for(
            operator.add, Bool, "not shown", unsupported_msg="{type_name} {op_name}"
        )


def test_invalid_custom_msg():
    with pytest.raises(TypeError, match="Bool Bool Int, Float"):
        check_valid_binop_for(
            operator.and_,
            Bool,
            "not shown",
            valid_result_types=(Int, Float),
            invalid_msg="{type_name} {result_name} {valid_result_names}",
        )
