import pytest
import numpy as np

from ...containers import Array, List, Tuple
from ...primitives import Float, Int
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


@pytest.mark.parametrize(
    "obj, expected_type",
    [
        (List[Array[Int, 1]]([]), List[Array[Int, 1]]),
        ([Array[Int, 1]([])], List[Array[Int, 1]]),
        ([Array[Int, 1]([]), Array[Int, 1]([])], List[Array[Int, 1]]),
        ([Array[Int, 1]([]), [1, 2]], List[Array[Int, 1]]),
        ([np.array([1, 2]), Array[Int, 1]([])], List[Array[Int, 1]]),
        ([np.array([[1, 2]]), np.array([[3, 2]])], List[Array[Int, 2]]),
    ],
)
def test_promote_to_list_of_same_arrays(obj, expected_type):
    promoted = wf_np.numpy_overrides._promote_to_list_of_same_arrays(obj, "frobnicate")
    assert isinstance(promoted, expected_type)


@pytest.mark.parametrize(
    "obj, error",
    [
        (Float(2.0), TypeError("Cannot frobnicate type Float")),
        (
            List[List[Float]]([]),
            TypeError(r"Cannot frobnicate type List\[List\[Float\]\]"),
        ),
        (2, TypeError("frobnicate expected an iterable")),
        (
            [np.array([1, 2]), "foo"],
            TypeError("Element 1 to frobnicate: expected an Array-like object"),
        ),
        (
            [Array[Int, 1]([]), Array[Int, 2]([[]])],
            ValueError(
                r"Cannot frobnicate Arrays of different dimensionality\. "
                r"Element 0 is 1-dimensional, and element 1 is 2-dimensional"
            ),
        ),
        (
            [Array[Int, 1]([]), Array[Float, 1]([])],
            TypeError(
                r"Cannot frobnicate Arrays with different dtypes\. "
                r"Element 0 has dtype Int, and element 1 has dtype Float"
            ),
        ),
        ([], ValueError("need at least one Array to frobnicate")),
    ],
)
def test_promote_to_list_of_same_arrays_errors(obj, error):
    with pytest.raises(type(error), match=error.args[0]):
        wf_np.numpy_overrides._promote_to_list_of_same_arrays(obj, "frobnicate")


@pytest.mark.parametrize(
    "bins, range, bins_return_type",
    [
        [5, (0, 10), Array[Float, 1]],
        [[1, 2, 3], None, Array[Float, 1]],
        [[0.5, 1.0, 1.5], None, Array[Float, 1]],
    ],
)
@pytest.mark.parametrize(
    "density, hist_return_type",
    [[None, Array[Int, 1]], [False, Array[Int, 1]], [True, Array[Float, 1]]],
)
def test_histogram(bins, range, bins_return_type, density, hist_return_type):
    result = wf_np.histogram(img_arr, bins=bins, range=range, density=density)
    assert isinstance(result, Tuple[hist_return_type, bins_return_type])


def test_histogram_bins_range_raises():
    with pytest.raises(ValueError):
        wf_np.histogram(img_arr, bins=5, range=None)

    with pytest.raises(ValueError):
        wf_np.histogram(img_arr, bins=5)


@pytest.mark.parametrize("density", ["foo", 5, object()])
def test_histogram_manual_density_typecheck(density):
    with pytest.raises(TypeError):
        wf_np.histogram(img_arr, bins=5, range=(0, 1), density=density)
