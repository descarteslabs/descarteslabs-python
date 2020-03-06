import operator

import pytest

from ...primitives import Int, Float, Bool
from ...geospatial import Image, ImageCollection
from .. import Array, Tuple


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
        (1, 2),
        ((1, None), 3),
        ([1, 2], 3),
        (Array[Int, 1]([1, 2]), 3),
        (slice(2), 3),
    ],
)
def test_getitem(idx, expected_ndim):
    arr = Array[Int, 3]([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]])
    assert arr[idx].ndim == expected_ndim


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
