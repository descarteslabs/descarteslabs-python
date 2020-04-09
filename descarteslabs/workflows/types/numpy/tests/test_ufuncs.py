import pytest
import numpy as np

from descarteslabs.workflows.types.array import Array, MaskedArray
from descarteslabs.workflows.types.primitives import Int, Float, Bool
from descarteslabs.workflows.types import proxify
import descarteslabs.workflows.types.numpy as wf_np


img_arr = Array(np.ones((3, 3, 3)))
col_arr = Array(np.ones((2, 3, 3, 3)))
data = np.ones((3, 3, 3))
mask = (np.arange(data.size) % 3 == 0).reshape(data.shape)
img_arr_masked = MaskedArray(data, mask)
data = np.ones((2, 3, 3, 3))
mask = (np.arange(data.size) % 3 == 0).reshape(data.shape)
col_arr_masked = MaskedArray(data, mask)


@pytest.mark.parametrize(
    "arg, return_ndim, return_type",
    [
        [img_arr, 3, Array],
        [img_arr_masked, 3, MaskedArray],
        [col_arr, 4, Array],
        [col_arr_masked, 4, MaskedArray],
        [Int(1), -1, None],
        [Float(2.2), -1, None],
        [Bool(True), -1, None],
        [1, -1, None],
        [2.2, -1, None],
        [True, -1, None],
    ],
)
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
def test_single_arg_methods(arg, return_ndim, return_type, operator):
    method = getattr(wf_np, operator)
    result = method(arg)

    if return_ndim >= 0:
        assert isinstance(result, return_type)
    else:
        assert isinstance(
            result,
            method._return_type_override
            if method._return_type_override is not None
            else type(proxify(arg)),
        )


@pytest.mark.parametrize(
    "args, return_ndim, return_type",
    [
        [[img_arr, img_arr], 3, Array],
        [[col_arr, img_arr], 4, Array],
        [[img_arr, col_arr], 4, Array],
        [[col_arr, col_arr], 4, Array],
        [[col_arr, 1], 4, Array],
        [[img_arr, 1], 3, Array],
        [[1, col_arr], 4, Array],
        [[1, img_arr], 3, Array],
        [[img_arr_masked, 1], 3, MaskedArray],
        [[1, img_arr_masked], 3, MaskedArray],
        [[img_arr_masked, img_arr_masked], 3, MaskedArray],
        [[img_arr_masked, img_arr], 3, MaskedArray],
        [[img_arr, img_arr_masked], 3, MaskedArray],
        [[img_arr_masked, col_arr], 4, MaskedArray],
        [[img_arr_masked, col_arr_masked], 4, MaskedArray],
        [[col_arr_masked, img_arr_masked], 4, MaskedArray],
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
def test_double_arg_methods(args, return_ndim, return_type, operator):
    method = getattr(wf_np, operator)
    result = method(*args)

    if return_ndim >= 0:
        assert isinstance(result, return_type)
    else:
        assert isinstance(
            result,
            method._return_type_override
            if method._return_type_override is not None
            else (Int, Float),
        )
