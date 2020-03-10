import operator

import pytest

from ...primitives import Int, Float, Bool, Str
from ...geospatial import Image, ImageCollection
from ...containers import Tuple, List
from .. import Array


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic Array"):
        Array([1, 2, 3])


def test_init():
    arr = Array[Int, 1]([1, 2, 3, 4])
    assert isinstance(arr, Array[Int, 1])
    assert arr.dtype is Int
    assert arr.ndim == 1


def test_validate_params():
    Array[Int, 1]
    Array[Float, 2]
    Array[Bool, 3]
    Array[Bool, 0]

    with pytest.raises(
        AssertionError, match="Both Array dtype and ndim must be specified"
    ):
        Array[Int]

    with pytest.raises(TypeError, match="dtype must be a Proxytype"):
        Array[1, 1]

    with pytest.raises(AssertionError, match="ndim must be a Python integer"):
        Array[Int, Int]
        Array[Int, "test"]

    with pytest.raises(AssertionError, match="Array ndim must be >= 0, not -1"):
        Array[Int, -1]


def test_dtype_ndim_shape():
    arr = Array[Int, 1]([1, 2, 3, 4])
    assert arr.dtype is Int
    assert arr.ndim == 1
    assert isinstance(arr.shape, Tuple[Int])


@pytest.mark.parametrize(
    "idx, expected_ndim",
    [
        (None, 4),
        ((0, 0, 0), 0),
        ((None, 0, 0, 0), 1),
        (1, 2),
        ((1, None), 3),
        ([1, 2], 3),
        (Array[Int, 1]([1, 2]), 3),
        (Array[Bool, 1]([True, False]), 3),
        (Array[Bool, 3]([[[]]]), 1),
        ((Array[Bool, 3]([[[]]]), None), 2),
        ((0, Array[Bool, 2]([[]])), 1),
        ((0, Array[Bool, 2]([[]]), None), 2),
        (slice(2), 3),
    ],
)
def test_getitem(idx, expected_ndim):
    arr = Array[Int, 3]([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]])
    result = arr[idx]
    if expected_ndim > 0:
        assert result.ndim == expected_ndim
    else:
        assert isinstance(result, arr.dtype)


@pytest.mark.parametrize(
    "idx, err_type, msg",
    [
        ((1, 2, 3, 4, 5), ValueError, r"Too many indicies \(5\) for a 4D Array"),
        ((None, 1, 2, 3, 4, 5), ValueError, r"Too many indicies \(5\) for a 4D Array"),
        ([1, 2.2], TypeError, r"Invalid types in \[1, 2.2\]"),
        (
            List[Str]([]),
            TypeError,
            r"only be sliced with 1D List\[Int\] or List\[Bool\], not List\[Str\]",
        ),
        (
            Array[Int, 2]([[]]),
            ValueError,
            "Slicing an Array with a multidimensional Array of Ints is not supported",
        ),
        (
            Array[Int, 0](1),
            ValueError,
            "tried to slice with a 0D Int Array, must be 1D",
        ),
        (
            (Array[Int, 1]([]), Array[Int, 1]([])),
            ValueError,
            "cannot slice an Array with lists or Arrays in multiple axes",
        ),
        (
            ([1], [2]),
            ValueError,
            "cannot slice an Array with lists or Arrays in multiple axes",
        ),
        (
            ([1], Array[Int, 1]([])),
            ValueError,
            "cannot slice an Array with lists or Arrays in multiple axes",
        ),
        (Array[Bool, 3]([[[]]]), ValueError, "must be 1D or 4D"),
        ((0, Array[Bool, 2]([[]])), ValueError, "must be 1D or 3D"),
    ],
)
def test_getitem_error(idx, err_type, msg):
    arr = Array[Int, 4]([[[[]]]])
    with pytest.raises(err_type, match=msg):
        arr[idx]


def test_to_imagery():
    arr = Array[Int, 2]([[10, 11, 12], [13, 14, 15]])

    with pytest.raises(ValueError, match="must be 3 or 4-dimensional"):
        arr.to_imagery({}, {})

    arr = Array[Int, 3]([[[]]])
    assert isinstance(arr.to_imagery({}, {}), Image)

    arr = Array[Int, 4]([[[[]]]])
    assert isinstance(arr.to_imagery({}, {}), ImageCollection)


@pytest.mark.parametrize("method", [operator.lt, operator.le, operator.gt, operator.ge])
@pytest.mark.parametrize("other", [Array[Int, 2]([[1, 2, 3], [4, 5, 6]]), 1, 0.5])
def test_container_bool_methods(method, other):
    arr = Array[Int, 2]([[10, 11, 12], [13, 14, 15]])
    result = method(arr, other)
    assert isinstance(result, Array[Bool, 2])


@pytest.mark.parametrize(
    "method",
    [
        operator.add,
        operator.sub,
        operator.mul,
        operator.floordiv,
        operator.truediv,
        operator.mod,
        operator.pow,
    ],
)
@pytest.mark.parametrize("other", [Array[Int, 2]([[1, 2, 3], [4, 5, 6]]), 1, 0.5])
def test_container_arithmetic(method, other):
    arr = Array[Float, 2]([[10.0, 11.0, 12.0], [13.0, 14.0, 15.0]])
    result = method(arr, other)
    r_result = method(other, arr)
    assert isinstance(result, Array[Float, 2])
    assert isinstance(r_result, Array[Float, 2])


@pytest.mark.parametrize(
    "axis, return_type", [(1, Array[Int, 2]), ((1, 2), Array[Int, 1]), ((1, 2, 3), Int)]
)
def test_stats(axis, return_type):
    arr = Array[Int, 3]([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]])

    assert isinstance(arr.min(axis=axis), return_type)
    assert isinstance(arr.max(axis=axis), return_type)
    assert isinstance(arr.mean(axis=axis), return_type)
    assert isinstance(arr.median(axis=axis), return_type)
    assert isinstance(arr.sum(axis=axis), return_type)
    assert isinstance(arr.std(axis=axis), return_type)
