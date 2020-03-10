import pytest
import numpy as np

from ...containers import Array
from ...primitives import Float
import descarteslabs.workflows.types.numpy as wf_np

img_arr = Array[Float, 3](np.ones((3, 3, 3)))
col_arr = Array[Float, 4](np.ones((2, 3, 3, 3)))


@pytest.mark.parametrize("args, return_ndim", [[[img_arr], 3], [[col_arr], 4]])
@pytest.mark.parametrize(
    "operator",
    [
        "negative",
        "exp",
        "exp2",
        "log",
        "log2",
        "log10",
        "log1p",
        "expm1",
        "sqrt",
        "square",
        "cbrt",
        "reciprocal",
        "sin",
        "cos",
        "tan",
        "arcsin",
        "arccos",
        "arctan",
        # "hypot",
        "sinh",
        "cosh",
        "tanh",
        "arcsinh",
        "arccosh",
        "arctanh",
        "deg2rad",
        "rad2deg",
        # "bitwise_not",
        "logical_not",
        "isfinite",
        "isinf",
        "isnan",
        "signbit",
        "spacing",
        "floor",
        "ceil",
        "trunc",
        "degrees",
        "radians",
        "rint",
        "fabs",
        "sign",
        "absolute",
    ],
)
def test_single_arg_methods(args, return_ndim, operator):
    method = getattr(wf_np, operator)
    result = method(*args)

    assert isinstance(result, Array)
    assert result.ndim == return_ndim


@pytest.mark.parametrize(
    "args, return_ndim",
    [
        [[img_arr, img_arr], 3],
        [[col_arr, img_arr], 4],
        [[img_arr, col_arr], 3],
        [[col_arr, col_arr], 4],
    ],
)
@pytest.mark.parametrize(
    "operator",
    [
        "add",
        "subtract",
        "multiply",
        "divide",
        "logaddexp",
        "logaddexp2",
        "true_divide",
        "floor_divide",
        "power",
        "float_power",
        "remainder",
        "mod",
        "arctan2",
        # "bitwise_and",
        # "bitwise_or",
        # "bitwise_xor",
        "greater",
        "greater_equal",
        "less",
        "less_equal",
        "not_equal",
        "equal",
        "logical_and",
        "logical_or",
        "logical_xor",
        "maximum",
        "minimum",
        "fmax",
        "fmin",
        "copysign",
        "nextafter",
        # "ldexp",
        "fmod",
    ],
)
def test_double_arg_methods(args, return_ndim, operator):
    method = getattr(wf_np, operator)
    result = method(*args)

    assert isinstance(result, Array)
    assert result.ndim == return_ndim
